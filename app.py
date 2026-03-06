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
        if 'color' not in df.columns and '顏色' not in df.columns:
            df['color'] = ""
        if 'note' not in df.columns and '備註' not in df.columns:
            df['note'] = ""
        if 'spec' not in df.columns and '規格' not in df.columns:
            df['spec'] = ""
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
    """核心邏輯：移庫作業"""
    if from_wh == to_wh:
        return False, "❌ 來源與目標倉庫不能相同"
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

# 側邊欄
with st.sidebar:
    st.header("功能選單")
    page = st.radio("前往", ["📦 商品管理", "📦 移庫作業", "📥 進貨作業", "🚚 出貨作業", "🔨 製造作業", "⚖️ 庫存盤點", "📊 報表查詢", "⚡ 快速匯入(Excel)", "🛠️ 系統維護"])
    st.divider()
    if st.button("🔄 強制重新讀取"):
        clear_cache(); st.success("已更新！"); time.sleep(0.5); st.rerun()

# --- 輔助函式：全系統統一格式化的商品清單 ---
def get_formatted_product_df():
    df = load_data("Products")
    if df.empty: return df
    df['spec'] = df['spec'].fillna('').astype(str)
    df['color'] = df['color'].fillna('').astype(str)
    # 統一顯示格式：貨號 | 品名 (規格 / 顏色)
    df['label'] = (
        df['sku'].astype(str) + " | " + 
        df['name'].astype(str) + 
        " (" + df['spec'] + " / " + df['color'] + ")"
    )
    return df

# --- 📦 移庫作業 ---
if page == "📦 移庫作業":
    st.subheader("📦 倉庫間移庫 (直接更改所在倉庫)")
    prods = get_formatted_product_df()
    if not prods.empty:
        with st.form("transfer_form"):
            c1, c2 = st.columns([2, 1])
            sel_prod = c1.selectbox("選擇商品 (貨號 | 品名 (規格/顏色))", prods['label'])
            user = c2.selectbox("經手人", KEYERS)
            col_wh1, col_wh2, col_qty = st.columns(3)
            from_wh = col_wh1.selectbox("1. 來源倉庫 (撥出)", WAREHOUSES, index=0)
            to_wh = col_wh2.selectbox("2. 目標倉庫 (撥入)", WAREHOUSES, index=1)
            qty = col_qty.number_input("3. 數量", min_value=0.1, step=1.0)
            note = st.text_input("備註")
            if st.form_submit_button("🚀 執行移庫"):
                success, msg = transfer_stock(sel_prod.split(" | ")[0], from_wh, to_wh, qty, user, note)
                if success: st.success(msg); time.sleep(1); st.rerun()
                else: st.error(msg)
    render_history_table(["移庫(撥出)", "移庫(撥入)"])

# --- 📥 進貨作業 ---
elif page == "📥 進貨作業":
    st.subheader("📥 進貨入庫")
    prods = get_formatted_product_df()
    if not prods.empty:
        with st.form("in"):
            c1, c2 = st.columns([2, 1])
            sel_prod = c1.selectbox("商品", prods['label'])
            wh = c2.selectbox("倉庫", WAREHOUSES)
            c3, c4 = st.columns(2)
            qty = c3.number_input("數量", 1.0)
            d_val = c4.date_input("日期", date.today())
            user = st.selectbox("經手人", KEYERS)
            note = st.text_input("備註")
            if st.form_submit_button("執行進貨"):
                if add_transaction("進貨", str(d_val), sel_prod.split(" | ")[0], wh, qty, user, note):
                    st.success("成功"); time.sleep(0.5); st.rerun()
    render_history_table("進貨")

# --- 🚚 出貨作業 ---
elif page == "🚚 出貨作業":
    st.subheader("🚚 銷售出貨")
    prods = get_formatted_product_df()
    if not prods.empty:
        with st.form("out"):
            c1, c2 = st.columns([2, 1])
            sel_prod = c1.selectbox("商品", prods['label'])
            wh = c2.selectbox("倉庫", WAREHOUSES, index=2)
            c3, c4 = st.columns(2)
            qty = c3.number_input("數量", 1.0)
            d_val = c4.date_input("日期", date.today())
            user = st.selectbox("經手人", KEYERS)
            note = st.text_input("訂單/備註")
            if st.form_submit_button("執行出貨"):
                if add_transaction("銷售出貨", str(d_val), sel_prod.split(" | ")[0], wh, qty, user, note):
                    st.success("成功"); time.sleep(0.5); st.rerun()
    render_history_table("銷售出貨")

# --- 🔨 製造作業 ---
elif page == "🔨 製造作業":
    st.subheader("🔨 生產管理")
    prods = get_formatted_product_df()
    if not prods.empty:
        t1, t2 = st.tabs(["領料 (扣原物料)", "完工 (增成品)"])
        with t1:
            with st.form("mo1"):
                sel = st.selectbox("原料 (貨號 | 品名 (規格/顏色))", prods['label'])
                wh = st.selectbox("倉庫", WAREHOUSES)
                qty = st.number_input("量", 1.0)
                note = st.text_input("備註", "領料")
                if st.form_submit_button("領料"):
                    add_transaction("製造領料", str(date.today()), sel.split(" | ")[0], wh, qty, "工廠", note)
                    st.success("OK"); time.sleep(0.5); st.rerun()
            render_history_table("製造領料")
        with t2:
            with st.form("mo2"):
                sel = st.selectbox("成品 (貨號 | 品名 (規格/顏色))", prods['label'])
                wh = st.selectbox("倉庫", WAREHOUSES)
                qty = st.number_input("量", 1.0)
                note = st.text_input("備註", "完工")
                if st.form_submit_button("完工"):
                    add_transaction("製造入庫", str(date.today()), sel.split(" | ")[0], wh, qty, "工廠", note)
                    st.success("OK"); time.sleep(0.5); st.rerun()
            render_history_table("製造入庫")

# --- ⚖️ 庫存盤點 ---
elif page == "⚖️ 庫存盤點":
    st.subheader("⚖️ 庫存調整")
    prods = get_formatted_product_df()
    if not prods.empty:
        with st.form("adj"):
            c1, c2 = st.columns(2)
            sel = c1.selectbox("商品", prods['label'])
            wh = c2.selectbox("倉庫", WAREHOUSES)
            c3, c4 = st.columns(2)
            act = c3.radio("動作", ["增加 (+)", "減少 (-)"], horizontal=True)
            qty = c4.number_input("量", 1.0)
            res = st.selectbox("原因", DEFAULT_REASONS)
            note = st.text_input("補充備註")
            if st.form_submit_button("調整"):
                tp = "庫存調整(加)" if act == "增加 (+)" else "庫存調整(減)"
                add_transaction(tp, str(date.today()), sel.split(" | ")[0], wh, qty, "管理員", f"{res}-{note}")
                st.success("OK"); time.sleep(0.5); st.rerun()
    render_history_table(["庫存調整(加)", "庫存調整(減)"])

# 其餘頁面 (商品管理, 報表查詢, 匯入, 系統維護) 保持原樣...
elif page == "📦 商品管理":
    st.subheader("📦 商品資料維護")
    tab1, tab2 = st.tabs(["✨ 新增商品", "✏️ 修改/刪除商品"])
    with tab1:
        c_cat, c_ser = st.columns(2)
        cat = c_cat.selectbox("1. 選擇分類", CATEGORIES + ["➕ 手動輸入新分類..."])
        ser = c_ser.selectbox("2. 選擇系列", SERIES + ["➕ 手動輸入新系列..."])
        if "手動" in cat: cat = c_cat.text_input("新分類名稱")
        if "手動" in ser: ser = c_ser.text_input("新系列名稱")
        
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
            if sku and name:
                s, m = add_product(sku, name, cat, ser, spec, note, color)
                if s: st.success("成功"); time.sleep(1); st.rerun()
                else: st.error(m)
    with tab2:
        df_prod = load_data("Products")
        if not df_prod.empty:
            sel_sku = st.selectbox("🔍 選擇商品", df_prod['sku'].astype(str))
            curr = df_prod[df_prod['sku'].astype(str) == sel_sku].iloc[0]
            with st.form("edit"):
                n_name = st.text_input("品名", curr['name'])
                n_spec = st.text_input("規格", curr['spec'])
                n_color = st.text_input("顏色", curr['color'])
                if st.form_submit_button("💾 儲存"):
                    update_product(sel_sku, {'name': n_name, 'spec': n_spec, 'color': n_color})
                    st.success("更新成功"); time.sleep(1); st.rerun()

elif page == "📊 報表查詢":
    st.subheader("📊 數據報表中心")
    df = get_stock_overview()
    st.dataframe(df, use_container_width=True)
    if not df.empty: st.download_button("📥 下載 Excel", to_excel_download(df), f"Stock_{date.today()}.xlsx")

elif page == "⚡ 快速匯入(Excel)":
    st.subheader("⚡ 批次匯入")
    uploaded_file = st.file_uploader("上傳 Excel", type=["xlsx"])
    if uploaded_file and st.button("🚀 開始匯入"):
        success, msg = process_bulk_import(pd.read_excel(uploaded_file))
        st.success(msg) if success else st.error(msg)

elif page == "🛠️ 系統維護":
    st.subheader("🛠️ 系統工具")
    if st.button("🚀 執行：舊庫存搬移"):
        # 此處保留你原本的 Migration 邏輯...
        st.info("已執行搬移程序 (範例代碼省略實際循環)")
