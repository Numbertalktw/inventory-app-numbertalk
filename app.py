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
    df = pd.DataFrame(data)
    if sheet_name == "Products" and not df.empty:
        for col in ['color', 'note', 'spec']:
            if col not in df.columns: df[col] = ""
    return df

def clear_cache():
    load_data.clear()

# ==========================================
# 3. 核心邏輯函式
# ==========================================

def generate_auto_sku(series, category, existing_skus_set):
    prefix = PREFIX_MAP.get(series, PREFIX_MAP.get(category, "XX"))
    count = 1
    while True:
        candidate = f"{prefix}-{count:03d}" 
        if candidate not in existing_skus_set:
            return candidate
        count += 1
        if count > 9999: return f"{prefix}-{int(time.time())}"

# --- ✨ 修正點：補齊 add_product 函式 ---
def add_product(sku, name, category, series, spec, note, color):
    df = load_data("Products")
    if not df.empty and str(sku) in df['sku'].astype(str).values:
        return False, "❌ 貨號已存在"
    
    ws = get_worksheet("Products")
    try:
        # 1. 寫入商品資料
        ws.append_row([str(sku), series, category, name, spec, color, note])
        
        # 2. 初始化庫存表 (Stock 分頁)
        ws_stock = get_worksheet("Stock")
        if ws_stock:
            new_stock_rows = [[str(sku), wh, 0.0] for wh in WAREHOUSES]
            ws_stock.append_rows(new_stock_rows)
            
        clear_cache()
        return True, "✅ 成功建立商品"
    except Exception as e:
        return False, f"新增失敗: {e}"

# --- ✨ 修正點：補齊 update_product 函式 ---
def update_product(sku, new_data):
    ws = get_worksheet("Products")
    try:
        cell = ws.find(str(sku))
        row = cell.row
        # A=1(sku), B=2(series), C=3(category), D=4(name), E=5(spec), F=6(color), G=7(note)
        if 'name' in new_data: ws.update_cell(row, 4, new_data['name'])
        if 'spec' in new_data: ws.update_cell(row, 5, new_data['spec'])
        if 'color' in new_data: ws.update_cell(row, 6, new_data['color'])
        if 'note' in new_data: ws.update_cell(row, 7, new_data['note'])
        clear_cache()
        return True, "✅ 更新成功"
    except Exception as e:
        return False, f"更新失敗: {e}"

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

def transfer_stock(sku, from_wh, to_wh, qty, user, note):
    if from_wh == to_wh: return False, "❌ 來源與目標倉庫不能相同"
    timestamp = str(datetime.now())
    today_str = str(date.today())
    doc_no = f"TR-{int(time.time())}"
    ws_hist = get_worksheet("History")
    record_out = ["移庫(撥出)", doc_no, today_str, str(sku), from_wh, float(qty), user, f"移至 {to_wh} | {note}", 0, timestamp]
    record_in = ["移庫(撥入)", doc_no, today_str, str(sku), to_wh, float(qty), user, f"來自 {from_wh} | {note}", 0, timestamp]
    try:
        ws_hist.append_rows([record_out, record_in])
        update_stock_qty(sku, from_wh, -float(qty))
        update_stock_qty(sku, to_wh, float(qty))
        clear_cache()
        return True, f"✅ 移庫完成！單號: {doc_no}"
    except Exception as e:
        return False, f"移庫失敗: {e}"

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
        cells = ws_hist.findall(doc_no)
        if not cells: return False, "找不到該單號"
        for cell in reversed(cells):
            row_num = cell.row
            record = ws_hist.row_values(row_num)
            r_type, r_sku, r_wh, r_qty = record[0], record[3], record[4], float(record[5])
            if r_type == "移庫(撥出)": update_stock_qty(r_sku, r_wh, r_qty)
            elif r_type == "移庫(撥入)": update_stock_qty(r_sku, r_wh, -r_qty)
            else:
                reverse_factor = 1 if r_type in ['銷售出貨', '製造領料', '庫存調整(減)'] else -1
                update_stock_qty(r_sku, r_wh, r_qty * reverse_factor)
            ws_hist.delete_rows(row_num)
        clear_cache()
        return True, "✅ 紀錄已刪除，庫存已還原。"
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
    
    cols_in_result = result.columns.tolist()
    final_wh_cols = []
    for w in WAREHOUSES:
        if f"{w}_y" in cols_in_result:
            result.rename(columns={f"{w}_y": w}, inplace=True)
            final_wh_cols.append(w)
        elif w in cols_in_result:
            final_wh_cols.append(w)
    target_cols = ['sku', 'series', 'category', 'name', 'spec', 'color', 'note', '總庫存'] + final_wh_cols
    return result[[c for c in target_cols if c in result.columns]]

def to_excel_download(df):
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False)
    return output.getvalue()

def render_history_table(doc_type_filter=None):
    st.markdown("#### 🕒 最近紀錄 (可刪除)")
    df = load_data("History")
    if df.empty:
        st.info("尚無紀錄"); return
    df_prod = load_data("Products")
    sku_map = {}
    if not df_prod.empty:
        sku_map = dict(zip(df_prod['sku'].astype(str), df_prod['name']))
    if doc_type_filter:
        if isinstance(doc_type_filter, list): df = df[df['doc_type'].isin(doc_type_filter)]
        else: df = df[df['doc_type'] == doc_type_filter]
    
    df = df.sort_index(ascending=False).head(10)
    cols = st.columns([1.5, 1.5, 3, 1, 1, 1, 2, 1])
    headers = ["單號", "日期", "品名 / SKU", "倉庫", "數量", "經手", "備註", "操作"]
    for col, h in zip(cols, headers): col.markdown(f"**{h}**")
    for _, row in df.iterrows():
        c1, c2, c3, c4, c5, c6, c7, c8 = st.columns([1.5, 1.5, 3, 1, 1, 1, 2, 1])
        c1.text(row.get('doc_no', '')[-10:])
        c2.text(row.get('date', ''))
        sku = str(row.get('sku',''))
        c3.text(f"{sku_map.get(sku, '未知')}\n({sku})")
        c4.text(row.get('warehouse', ''))
        c5.text(row.get('qty', 0))
        c6.text(row.get('user', ''))
        c7.text(row.get('note', ''))
        if c8.button("🗑️", key=f"del_{row['doc_no']}_{row['warehouse']}"):
            with st.spinner("刪除中..."):
                success, msg = delete_transaction(row['doc_no'])
                if success: st.success(msg); time.sleep(1); st.rerun()
                else: st.error(msg)
        st.divider()

# ==========================================
# 5. 主程式介面
# ==========================================
st.set_page_config(page_title=PAGE_TITLE, layout="wide", page_icon="💎")
st.title(f"💎 {PAGE_TITLE}")

if "gcp_service_account" not in st.secrets:
    st.error("❌ 未偵測到 secrets 設定。"); st.stop()
if not get_client(): st.stop()

with st.sidebar:
    st.header("功能選單")
    page = st.radio("前往", ["📦 商品管理", "📦 移庫作業", "📥 進貨作業", "🚚 出貨作業", "🔨 製造作業", "⚖️ 庫存盤點", "📊 報表查詢", "⚡ 快速匯入(Excel)", "🛠️ 系統維護"])
    st.divider()
    if st.button("🔄 強制重新讀取"):
        clear_cache(); st.success("已更新！"); time.sleep(0.5); st.rerun()

def get_formatted_product_df():
    df = load_data("Products")
    if df.empty: return df
    df['spec'] = df['spec'].fillna('').astype(str)
    df['color'] = df['color'].fillna('').astype(str)
    df['label'] = df['sku'].astype(str) + " | " + df['name'].astype(str) + " (" + df['spec'] + " / " + df['color'] + ")"
    return df

# --- 📦 商品管理 ---
if page == "📦 商品管理":
    st.subheader("📦 商品資料維護")
    tab1, tab2 = st.tabs(["✨ 新增商品", "✏️ 修改/刪除商品"])
    with tab1:
        c_cat, c_ser = st.columns(2)
        cat_options = CATEGORIES + ["➕ 手動輸入新分類..."]
        sel_cat_opt = c_cat.selectbox("1. 選擇分類", cat_options)
        cat = c_cat.text_input("新分類名稱") if sel_cat_opt == "➕ 手動輸入新分類..." else sel_cat_opt
        
        ser_options = SERIES + ["➕ 手動輸入新系列..."]
        sel_ser_opt = c_ser.selectbox("2. 選擇系列", ser_options)
        ser = c_ser.text_input("新系列名稱") if sel_ser_opt == "➕ 手動輸入新系列..." else sel_ser_opt
        
        current_df = load_data("Products")
        auto_sku = generate_auto_sku(ser, cat, set(current_df['sku'].astype(str)) if not current_df.empty else set())
        
        c1, c2 = st.columns(2)
        sku = c1.text_input("3. 貨號", value=auto_sku)
        name = c2.text_input("4. 品名 *必填")
        c3, c4 = st.columns(2)
        spec = c3.text_input("5. 規格 (形狀/尺寸)")
        color = c4.text_input("6. 顏色")
        note = st.text_input("7. 備註")
        
        if st.button("✨ 確認新增"):
            if sku and name and cat and ser:
                s, m = add_product(sku, name, cat, ser, spec, note, color)
                if s: st.success(m); time.sleep(1); st.rerun()
                else: st.error(m)
            else: st.error("❌ 請填寫必填欄位 (貨號, 品名, 分類, 系列)")

    with tab2:
        df_prod = load_data("Products")
        if not df_prod.empty:
            sel_sku = st.selectbox("🔍 選擇商品", df_prod['sku'].astype(str))
            curr = df_prod[df_prod['sku'].astype(str) == sel_sku].iloc[0]
            with st.form("edit"):
                n_name = st.text_input("品名", curr['name'])
                n_spec = st.text_input("規格", curr.get('spec', ''))
                n_color = st.text_input("顏色", curr.get('color', ''))
                n_note = st.text_input("備註", curr.get('note', ''))
                if st.form_submit_button("💾 儲存修改"):
                    s, m = update_product(sel_sku, {'name': n_name, 'spec': n_spec, 'color': n_color, 'note': n_note})
                    if s: st.success(m); time.sleep(1); st.rerun()
                    else: st.error(m)

# --- 其餘頁面功能代碼省略以求簡潔，邏輯同前 ---
elif page == "📦 移庫作業":
    st.subheader("📦 移庫作業")
    prods = get_formatted_product_df()
    if not prods.empty:
        with st.form("transfer"):
            c1, c2 = st.columns([2, 1])
            sel_p = c1.selectbox("商品", prods['label'])
            user = c2.selectbox("經手", KEYERS)
            w1, w2, q = st.columns(3)
            f_w = w1.selectbox("從", WAREHOUSES)
            t_w = w2.selectbox("到", WAREHOUSES, index=1)
            qty = q.number_input("數量", 1.0)
            if st.form_submit_button("執行"):
                s, m = transfer_stock(sel_p.split(" | ")[0], f_w, t_w, qty, user, "手動移庫")
                if s: st.success(m); time.sleep(1); st.rerun()
                else: st.error(m)
    render_history_table(["移庫(撥出)", "移庫(撥入)"])

elif page == "📊 報表查詢":
    st.subheader("📊 數據報表")
    df = get_stock_overview()
    st.dataframe(df, use_container_width=True)
    if not df.empty: st.download_button("📥 下載", to_excel_download(df), f"Stock_{date.today()}.xlsx")
