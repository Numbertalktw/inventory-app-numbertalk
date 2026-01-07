import streamlit as st
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import date, datetime
import time
import io
import re

# ==========================================
# 1. 系統設定
# ==========================================
PAGE_TITLE = "numbertalk 雲端庫存系統"
SPREADSHEET_NAME = "numbertalk-system" 

# 固定選項
WAREHOUSES = ["Wen", "千畇", "James", "Imeng"]
CATEGORIES = ["天然石", "金屬配件", "線材", "包裝材料", "完成品", "數字珠", "數字串", "香料", "手作設備"]
SERIES = ["原料", "半成品", "成品", "包材", "生命數字能量項鍊", "數字手鍊", "貼紙", "小卡", "火漆章", "能量蠟燭", "香包", "水晶", "魔法鹽"]
KEYERS = ["Wen", "千畇", "James", "Imeng", "小幫手"]
DEFAULT_REASONS = ["盤點差異", "報廢", "樣品借出", "系統修正", "其他"]

# ★ [智慧編碼設定]
PREFIX_MAP = {
    "生命數字能量項鍊": "SN", "數字手鍊": "SB", "貼紙": "ST", "小卡": "CD",
    "火漆章": "FS", "能量蠟燭": "LA", "香包": "SB", "水晶": "CT", "魔法鹽": "MS",
    "天然石": "NS", "金屬配件": "MT", "線材": "WR", "包裝材料": "PK", "完成品": "PD"
}

# ==========================================
# 2. Google Sheet 連線核心
# ==========================================
SCOPE = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']

@st.cache_resource
def get_client():
    try:
        creds_dict = st.secrets["gcp_service_account"]
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, SCOPE)
        client = gspread.authorize(creds)
        return client
    except Exception as e:
        st.error(f"❌ 連線失敗: {e}")
        return None

def get_worksheet(sheet_name):
    client = get_client()
    if not client: return None
    try:
        sh = client.open(SPREADSHEET_NAME)
        return sh.worksheet(sheet_name)
    except Exception as e:
        st.error(f"❌ 讀取錯誤 ({sheet_name}): {e}")
        return None

@st.cache_data(ttl=5) 
def load_data(sheet_name):
    ws = get_worksheet(sheet_name)
    if not ws: return pd.DataFrame()
    data = ws.get_all_records()
    return pd.DataFrame(data)

def clear_cache():
    load_data.clear()

# ==========================================
# 3. 核心邏輯函式
# ==========================================

# --- [自動編碼] ---
def generate_auto_sku(series, category, existing_skus_set):
    prefix = PREFIX_MAP.get(series, PREFIX_MAP.get(category, "XX"))
    count = 1
    while True:
        candidate = f"{prefix}-{count:03d}" 
        if candidate not in existing_skus_set:
            return candidate
        count += 1
        if count > 9999: return f"{prefix}-{int(time.time())}"

# --- [批次匯入] ---
def process_bulk_import(df_upload):
    client = get_client()
    sh = client.open(SPREADSHEET_NAME)
    ws_prod = sh.worksheet("Products")
    ws_stock = sh.worksheet("Stock")
    ws_hist = sh.worksheet("History")

    existing_prods = ws_prod.get_all_records()
    existing_skus = set([str(p['sku']) for p in existing_prods])
    
    new_prods = []
    new_stocks = []
    new_hists = []
    timestamp = str(datetime.now())
    today_str = str(date.today())
    
    progress_bar = st.progress(0)
    status_text = st.empty()
    total_rows = len(df_upload)
    
    for idx, row in df_upload.iterrows():
        progress = (idx + 1) / total_rows
        progress_bar.progress(progress)
        status_text.text(f"正在處理第 {idx+1}/{total_rows} 筆...")

        u_sku = str(row.get('貨號 (SKU)', '')).strip()
        if u_sku == 'nan': u_sku = ''
        series = str(row.get('系列', '')).strip()
        category = str(row.get('分類', '')).strip()
        name = str(row.get('品名', '')).strip()
        spec = str(row.get('規格', '')).strip()
        note = str(row.get('備註', '')).strip()
        wh = str(row.get('倉庫', '')).strip()
        qty = float(row.get('數量', 0))
        cost = float(row.get('成本', 0))

        if not name: continue

        if not u_sku:
            u_sku = generate_auto_sku(series, category, existing_skus)
            existing_skus.add(u_sku)
        else:
            existing_skus.add(u_sku)

        is_new_prod_in_sheet = u_sku not in [str(p['sku']) for p in existing_prods]
        is_new_prod_in_batch = u_sku not in [p[0] for p in new_prods]

        if is_new_prod_in_sheet and is_new_prod_in_batch:
            new_prods.append([u_sku, name, category, series, spec, note])
            for w in WAREHOUSES:
                new_stocks.append([u_sku, w, 0.0])

        if qty > 0:
            if wh not in WAREHOUSES: wh = "Wen"
            doc_no = f"OPEN-{int(time.time())}-{idx}"
            new_hists.append(["期初建檔", doc_no, today_str, u_sku, wh, qty, "匯入", "批次匯入", cost, timestamp])
            new_stocks.append([u_sku, wh, qty])

    if new_prods: ws_prod.append_rows(new_prods)
    if new_stocks: ws_stock.append_rows(new_stocks)
    if new_hists: ws_hist.append_rows(new_hists)

    progress_bar.progress(1.0)
    clear_cache()
    return True, f"成功匯入！(新商品: {len(new_prods)} 筆)"

# --- 基本增刪改查 ---
def add_product(sku, name, category, series, spec, note):
    df = load_data("Products")
    if not df.empty and str(sku) in df['sku'].astype(str).values:
        return False, "貨號已存在"
    ws = get_worksheet("Products")
    ws.append_row([str(sku), name, category, series, spec, note])
    ws_stock = get_worksheet("Stock")
    if ws_stock:
        for wh in WAREHOUSES:
            ws_stock.append_row([str(sku), wh, 0.0])
    clear_cache()
    return True, "成功"

def update_product(sku, new_data):
    ws = get_worksheet("Products")
    try:
        cell = ws.find(str(sku))
        row = cell.row
        if 'name' in new_data: ws.update_cell(row, 2, new_data['name'])
        if 'category' in new_data: ws.update_cell(row, 3, new_data['category'])
        if 'series' in new_data: ws.update_cell(row, 4, new_data['series'])
        if 'spec' in new_data: ws.update_cell(row, 5, new_data['spec'])
        if 'note' in new_data: ws.update_cell(row, 6, new_data['note'])
        clear_cache()
        return True, "更新成功"
    except Exception as e:
        return False, f"更新失敗: {e}"

def delete_product(sku):
    ws = get_worksheet("Products")
    try:
        cell = ws.find(str(sku))
        ws.delete_rows(cell.row)
        clear_cache()
        return True, "商品已刪除"
    except Exception as e:
        return False, f"刪除失敗: {e}"

def update_stock_qty(sku, warehouse, delta_qty):
    ws = get_worksheet("Stock")
    if not ws: return
    try:
        all_vals = ws.get_all_values()
        if not all_vals: return
        header = all_vals[0]
        sku_idx, wh_idx, qty_idx = header.index("sku"), header.index("warehouse"), header.index("qty")
        row_to_update = -1
        current_val = 0.0
        for i, row in enumerate(all_vals):
            if i == 0: continue
            if str(row[sku_idx]) == str(sku) and str(row[wh_idx]) == str(warehouse):
                row_to_update = i + 1
                current_val = float(row[qty_idx]) if row[qty_idx] else 0.0
                break
        if row_to_update > 0:
            ws.update_cell(row_to_update, qty_idx + 1, current_val + delta_qty)
        else:
            ws.append_row([str(sku), warehouse, delta_qty])
    except Exception as e:
        st.error(f"更新庫存失敗: {e}")

def add_transaction(doc_type, date_str, sku, wh, qty, user, note, cost=0):
    ws_hist = get_worksheet("History")
    doc_prefix = {"進貨":"IN", "銷售出貨":"OUT", "製造領料":"MO", "製造入庫":"PD", "庫存調整(加)":"ADJ+", "庫存調整(減)":"ADJ-", "期初建檔":"OPEN"}.get(doc_type, "DOC")
    doc_no = f"{doc_prefix}-{int(time.time())}"
    ws_hist.append_row([doc_type, doc_no, str(date_str), str(sku), wh, float(qty), user, note, float(cost), str(datetime.now())])
    factor = -1 if doc_type in ['銷售出貨', '製造領料', '庫存調整(減)'] else 1
    update_stock_qty(sku, wh, float(qty) * factor)
    clear_cache()
    return True

def delete_transaction(doc_no):
    ws_hist = get_worksheet("History")
    try:
        cell = ws_hist.find(doc_no)
        row_num = cell.row
        record = ws_hist.row_values(row_num)
        r_type, r_sku, r_wh, r_qty = record[0], record[3], record[4], float(record[5])
        reverse_factor = 1 if r_type in ['銷售出貨', '製造領料', '庫存調整(減)'] else -1
        update_stock_qty(r_sku, r_wh, r_qty * reverse_factor)
        ws_hist.delete_rows(row_num)
        clear_cache()
        return True, "✅ 紀錄已刪除，庫存已自動還原。"
    except Exception as e:
        return False, f"刪除失敗: {e}"

def get_stock_overview():
    df_prod = load_data("Products")
    df_stock = load_data("Stock")
    if df_prod.empty: return pd.DataFrame()
    df_prod['sku'] = df_prod['sku'].astype(str)
    
    if df_stock.empty:
        result = df_prod.copy()
        for wh in WAREHOUSES: result[wh] = 0.0
        result['總庫存'] = 0.0
    else:
        df_stock['sku'] = df_stock['sku'].astype(str)
        df_stock['qty'] = pd.to_numeric(df_stock['qty'], errors='coerce').fillna(0)
        pivot = df_stock.pivot_table(index='sku', columns='warehouse', values='qty', aggfunc='sum').fillna(0)
        for wh in WAREHOUSES:
            if wh not in pivot.columns: pivot[wh] = 0.0
        pivot['總庫存'] = pivot[WAREHOUSES].sum(axis=1)
        result = pd.merge(df_prod, pivot, on='sku', how='left').fillna(0)
    
    target_cols = ['sku', 'series', 'category', 'name', 'spec', 'note', '總庫存'] + WAREHOUSES
    final_cols = [c for c in target_cols if c in result.columns]
    return result[final_cols]

def to_excel_download(df):
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False)
    return output.getvalue()

# ★ 修改這裡：顯示商品名稱
def render_history_table(doc_type_filter=None):
    st.markdown("#### 🕒 最近紀錄 (可刪除)")
    df = load_data("History")
    if df.empty:
        st.info("尚無紀錄")
        return
        
    # 預先讀取商品名稱
    df_prod = load_data("Products")
    sku_map = {}
    if not df_prod.empty:
        # 建立一個 SKU 對應 Name 的字典
        sku_map = dict(zip(df_prod['sku'].astype(str), df_prod['name']))

    if doc_type_filter:
        if isinstance(doc_type_filter, list):
            df = df[df['doc_type'].isin(doc_type_filter)]
        else:
            df = df[df['doc_type'] == doc_type_filter]
    
    df = df.sort_index(ascending=False).head(10)

    # 調整欄寬：品名欄位給大一點 (3)
    cols = st.columns([1.5, 1.5, 3, 1, 1, 1, 2, 1])
    headers = ["單號", "日期", "品名 / SKU", "倉庫", "數量", "經手", "備註", "操作"]
    for col, h in zip(cols, headers): col.markdown(f"**{h}**")
    
    for _, row in df.iterrows():
        c1, c2, c3, c4, c5, c6, c7, c8 = st.columns([1.5, 1.5, 3, 1, 1, 1, 2, 1])
        c1.text(row.get('doc_no', '')[-10:])
        c2.text(row.get('date', ''))
        
        # 顯示品名 + SKU
        sku = str(row.get('sku',''))
        prod_name = sku_map.get(sku, "未知品名")
        c3.text(f"{prod_name}\n({sku})")
        
        c4.text(row.get('warehouse', ''))
        c5.text(row.get('qty', 0))
        c6.text(row.get('user', ''))
        c7.text(row.get('note', ''))
        
        if c8.button("🗑️", key=f"del_{row['doc_no']}"):
            with st.spinner("刪除中..."):
                success, msg = delete_transaction(row['doc_no'])
                if success:
                    st.success(msg); time.sleep(1); st.rerun()
                else: st.error(msg)
        st.divider()

# ==========================================
# 5. 主程式介面
# ==========================================
st.set_page_config(page_title=PAGE_TITLE, layout="wide", page_icon="💎")
st.title(f"💎 {PAGE_TITLE}")

if "gcp_service_account" not in st.secrets:
    st.error("❌ 未偵測到 secrets 設定。")
    st.stop()
if not get_client(): st.stop()

with st.sidebar:
    st.header("功能選單")
    page = st.radio("前往", ["📦 商品管理", "📥 進貨作業", "🚚 出貨作業", "🔨 製造作業", "⚖️ 庫存盤點", "📊 報表查詢", "⚡ 快速匯入(Excel)", "🛠️ 系統維護"])
    st.divider()
    if st.button("🔄 強制重新讀取"):
        clear_cache()
        st.success("已更新！"); time.sleep(0.5); st.rerun()

# --- 🛠️ 系統維護 ---
if page == "🛠️ 系統維護":
    st.subheader("🛠️ 系統工具箱")
    st.info("此功能僅在導入初期使用，用於將 Google Sheet 上 Products 分頁的舊庫存數字，搬移至資料庫中。")
    if st.button("🚀 執行：舊庫存搬移至資料庫 (Data Migration)"):
        with st.spinner("正在搬移..."):
            ws_prod = get_worksheet("Products")
            prods = ws_prod.get_all_records()
            ws_stock = get_worksheet("Stock")
            stocks = ws_stock.get_all_records()
            existing_skus = set([str(s['sku']) for s in stocks])
            new_stocks = []
            new_hists = []
            timestamp = str(datetime.now())
            today_str = str(date.today())
            count = 0
            for p in prods:
                sku = str(p.get('sku'))
                if sku in existing_skus: continue
                for wh in ["Wen", "千畇", "James", "Imeng"]:
                    try:
                        qty = float(p.get(wh)) if p.get(wh) else 0
                    except: qty = 0
                    if qty != 0:
                        new_stocks.append([sku, wh, qty])
                        doc_no = f"MIG-{int(time.time())}-{count}"
                        new_hists.append(["期初導入", doc_no, today_str, sku, wh, qty, "系統", "舊資料自動搬移", 0, timestamp])
                        count += 1
            if new_stocks:
                ws_stock.append_rows(new_stocks)
                get_worksheet("History").append_rows(new_hists)
                st.balloons()
                st.success(f"🎉 成功搬移了 {len(new_stocks)} 筆資料！")
            else:
                st.info("沒有需要搬移的資料。")

# --- ⚡ 快速匯入(Excel) ---
elif page == "⚡ 快速匯入(Excel)":
    st.subheader("⚡ 批次匯入期初資料")
    with st.expander("📖 使用說明 & 範例下載"):
        st.markdown("""
        **請準備 Excel 檔案，欄位順序如下 (標題要一樣)：**
        1. `貨號 (SKU)` : 若留空，系統會自動編碼。若已有舊編號請填入。
        2. `系列` : 例如：貼紙、生命數字能量項鍊... (用來自動編碼)
        3. `分類` : 例如：包裝材料、完成品
        4. `品名` : *必填
        5. `規格`
        6. `備註`
        7. `倉庫` : 請填 Wen / 千畇 / James / Imeng
        8. `數量` : 期初庫存量
        9. `成本` : 單價 (選填)
        """)
        sample_data = pd.DataFrame([
            {"貨號 (SKU)":"", "系列":"貼紙", "分類":"包裝材料", "品名":"測試貼紙", "規格":"大", "備註":"", "倉庫":"Wen", "數量":100, "成本":1},
            {"貨號 (SKU)":"OLD-001", "系列":"完成品", "分類":"完成品", "品名":"舊商品範例", "規格":"", "備註":"舊貨", "倉庫":"Imeng", "數量":5, "成本":500},
        ])
        st.download_button("📥 下載 Excel 範例檔", to_excel_download(sample_data), "import_template.xlsx")
    uploaded_file = st.file_uploader("上傳 Excel 檔案 (.xlsx)", type=["xlsx"])
    if uploaded_file:
        df_up = pd.read_excel(uploaded_file)
        st.dataframe(df_up.head())
        if st.button("🚀 開始匯入"):
            success, msg = process_bulk_import(df_up)
            if success:
                st.success(msg); st.balloons()
            else:
                st.error(msg)

# --- (功能頁面) ---
elif page == "📦 商品管理":
    st.subheader("📦 商品資料維護")
    tab1, tab2 = st.tabs(["✨ 新增商品", "✏️ 修改/刪除商品"])
    
    with tab1:
        with st.form("add_prod"):
            st.info("💡 請先選擇 [分類] 與 [系列]，系統會自動帶入建議貨號。")
            c_cat, c_ser = st.columns(2)
            cat = c_cat.selectbox("1. 選擇分類", CATEGORIES)
            ser = c_ser.selectbox("2. 選擇系列", SERIES)
            
            try:
                current_df = load_data("Products")
                current_skus = set(current_df['sku'].astype(str).tolist())
            except:
                current_skus = set()
            
            auto_sku = generate_auto_sku(ser, cat, current_skus)
            
            c1, c2 = st.columns(2)
            sku = c1.text_input("3. 貨號 (可手動修改)", value=auto_sku)
            name = c2.text_input("4. 品名 *必填")
            c3, c4 = st.columns(2)
            spec = c3.text_input("規格/尺寸")
            note = c4.text_input("備註 (Note)")
            st.markdown("---")
            st.caption("👇 期初庫存 (選填)")
            c6, c7 = st.columns(2)
            init_wh = c6.selectbox("預設倉庫", WAREHOUSES)
            init_qty = c7.number_input("數量", min_value=0)

            if st.form_submit_button("新增商品"):
                if sku and name:
                    success, msg = add_product(sku, name, cat, ser, spec, note)
                    if success:
                        if init_qty > 0:
                            add_transaction("期初建檔", str(date.today()), sku, init_wh, init_qty, "系統", "新商品期初")
                        st.success(f"成功！已建立 {sku}"); time.sleep(1); st.rerun()
                    else: st.error(msg)
                else: st.error("缺必填欄位")

    with tab2:
        df_prod = load_data("Products")
        if not df_prod.empty:
            all_skus = df_prod['sku'].astype(str).tolist()
            sel_sku = st.selectbox("🔍 請選擇要修改或刪除的商品", all_skus)
            curr_data = df_prod[df_prod['sku'].astype(str) == sel_sku].iloc[0]
            with st.form("edit_prod"):
                st.info(f"正在編輯: {sel_sku} | {curr_data['name']}")
                new_name = st.text_input("品名", curr_data['name'])
                c1, c2, c3 = st.columns(3)
                new_cat = c1.selectbox("分類", CATEGORIES, index=CATEGORIES.index(curr_data['category']) if curr_data['category'] in CATEGORIES else 0)
                new_ser = c2.selectbox("系列", SERIES, index=SERIES.index(curr_data['series']) if curr_data['series'] in SERIES else 0)
                new_spec = c3.text_input("規格", curr_data['spec'])
                new_note = st.text_input("備註", curr_data.get('note', ''))
                c_edit, c_del = st.columns([4, 1])
                if c_edit.form_submit_button("💾 儲存修改"):
                    up_data = {'name': new_name, 'category': new_cat, 'series': new_ser, 'spec': new_spec, 'note': new_note}
                    s, m = update_product(sel_sku, up_data)
                    if s: st.success(m); time.sleep(1); st.rerun()
                    else: st.error(m)
                if c_del.form_submit_button("🗑️ 刪除此商品"):
                    s, m = delete_product(sel_sku)
                    if s: st.warning(m); time.sleep(1); st.rerun()
                    else: st.error(m)
        else: st.info("尚無商品資料")
    st.divider()
    st.dataframe(load_data("Products"), use_container_width=True)

elif page == "📥 進貨作業":
    st.subheader("📥 進貨入庫")
    prods = load_data("Products")
    if not prods.empty:
        prods['label'] = prods['sku'].astype(str) + " | " + prods['name']
        with st.form("in"):
            c1, c2 = st.columns([2, 1])
            sel_prod = c1.selectbox("商品", prods['label'])
            wh = c2.selectbox("倉庫", WAREHOUSES)
            c3, c4 = st.columns(2)
            qty = c3.number_input("數量", 1)
            d_val = c4.date_input("日期", date.today())
            user = st.selectbox("經手人", KEYERS)
            note = st.text_input("備註")
            if st.form_submit_button("進貨"):
                if add_transaction("進貨", str(d_val), sel_prod.split(" | ")[0], wh, qty, user, note):
                    st.success("成功"); time.sleep(0.5); st.rerun()
    render_history_table("進貨")

elif page == "🚚 出貨作業":
    st.subheader("🚚 銷售出貨")
    prods = load_data("Products")
    if not prods.empty:
        prods['label'] = prods['sku'].astype(str) + " | " + prods['name']
        with st.form("out"):
            c1, c2 = st.columns([2, 1])
            sel_prod = c1.selectbox("商品", prods['label'])
            wh = c2.selectbox("倉庫", WAREHOUSES, index=2)
            c3, c4 = st.columns(2)
            qty = c3.number_input("數量", 1)
            d_val = c4.date_input("日期", date.today())
            user = st.selectbox("經手人", KEYERS)
            note = st.text_input("訂單/備註")
            if st.form_submit_button("出貨"):
                if add_transaction("銷售出貨", str(d_val), sel_prod.split(" | ")[0], wh, qty, user, note):
                    st.success("成功"); time.sleep(0.5); st.rerun()
    render_history_table("銷售出貨")

elif page == "🔨 製造作業":
    st.subheader("🔨 生產管理")
    prods = load_data("Products")
    if not prods.empty:
        prods['label'] = prods['sku'].astype(str) + " | " + prods['name']
        t1, t2 = st.tabs(["領料 (扣原物料)", "完工 (增成品)"])
        with t1:
            with st.form("mo1"):
                sel = st.selectbox("原料", prods['label'])
                wh = st.selectbox("倉庫", WAREHOUSES)
                qty = st.number_input("量", 1)
                note = st.text_input("備註", "領料")
                if st.form_submit_button("領料"):
                    add_transaction("製造領料", str(date.today()), sel.split(" | ")[0], wh, qty, "工廠", note)
                    st.success("OK"); time.sleep(0.5); st.rerun()
            render_history_table("製造領料")
        with t2:
            with st.form("mo2"):
                sel = st.selectbox("成品", prods['label'])
                wh = st.selectbox("倉庫", WAREHOUSES)
                qty = st.number_input("量", 1)
                note = st.text_input("備註", "完工")
                if st.form_submit_button("完工"):
                    add_transaction("製造入庫", str(date.today()), sel.split(" | ")[0], wh, qty, "工廠", note)
                    st.success("OK"); time.sleep(0.5); st.rerun()
            render_history_table("製造入庫")

elif page == "⚖️ 庫存盤點":
    st.subheader("⚖️ 庫存調整")
    prods = load_data("Products")
    if not prods.empty:
        prods['label'] = prods['sku'].astype(str) + " | " + prods['name']
        with st.form("adj"):
            c1, c2 = st.columns(2)
            sel = c1.selectbox("商品", prods['label'])
            wh = c2.selectbox("倉庫", WAREHOUSES)
            c3, c4 = st.columns(2)
            act = c3.radio("動作", ["增加 (+)", "減少 (-)"], horizontal=True)
            qty = c4.number_input("量", 1)
            res = st.selectbox("原因", DEFAULT_REASONS)
            note = st.text_input("補充備註")
            full_note = f"{res} - {note}" if note else res
            if st.form_submit_button("調整"):
                tp = "庫存調整(加)" if act == "增加 (+)" else "庫存調整(減)"
                add_transaction(tp, str(date.today()), sel.split(" | ")[0], wh, qty, "管理員", full_note)
                st.success("OK"); time.sleep(0.5); st.rerun()
    st.divider()
    render_history_table(["庫存調整(加)", "庫存調整(減)"])

elif page == "📊 報表查詢":
    st.subheader("📊 數據報表中心")
    df = get_stock_overview()
    st.dataframe(df, use_container_width=True)
    if not df.empty:
        st.download_button("📥 下載 Excel", to_excel_download(df), f"Stock_Report_{date.today()}.xlsx")
