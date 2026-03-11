import streamlit as st
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import date, datetime
import time
import io

# ==========================================
# 1. 系統設定
# ==========================================
PAGE_TITLE = "numbertalk 雲端庫存系統"
SPREADSHEET_NAME = "numbertalk-system" 

WAREHOUSES = ["Wen", "千畇", "James", "Imeng"]
CATEGORIES = ["天然石", "金屬配件", "線材", "包裝材料", "完成品", "數字珠", "數字串", "香料", "手作設備"]
SERIES = ["原料", "半成品", "成品", "包材", "生命數字能量項鍊", "數字手鍊", "貼紙", "小卡", "火漆章", "能量蠟燭", "香包", "水晶", "魔法鹽"]
KEYERS = ["Wen", "千畇", "James", "Imeng", "小幫手"]
DEFAULT_REASONS = ["盤點差異", "報廢", "樣品借出", "系統修正", "其他"]

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
    try:
        data = ws.get_all_records()
        df = pd.DataFrame(data)
        # 強化穩定性：補齊欄位並填充空值
        for col in ['sku', 'name', 'category', 'series', 'spec', 'color', 'note']:
            if col not in df.columns: df[col] = ""
        return df.fillna("")
    except Exception:
        return pd.DataFrame()

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
        if count > 999: return f"{prefix}-{int(time.time())}"

def add_product(sku, name, category, series, spec, note, color):
    df = load_data("Products")
    if not df.empty and str(sku) in df['sku'].astype(str).values:
        return False, "❌ 貨號已存在"
    ws = get_worksheet("Products")
    try:
        ws.append_row([str(sku), series, category, name, spec, color, note])
        ws_stock = get_worksheet("Stock")
        if ws_stock:
            ws_stock.append_rows([[str(sku), wh, 0.0] for wh in WAREHOUSES])
        clear_cache()
        return True, "✅ 商品新增成功"
    except Exception as e:
        return False, f"新增失敗: {e}"

def update_product(sku, new_data):
    ws = get_worksheet("Products")
    try:
        cell = ws.find(str(sku))
        row = cell.row
        # D=4(name), E=5(spec), F=6(color), G=7(note)
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
        header = all_vals[0]
        sku_idx, wh_idx, qty_idx = header.index("sku"), header.index("warehouse"), header.index("qty")
        row_idx = -1
        current_val = 0.0
        for i, row in enumerate(all_vals[1:], 2):
            if str(row[sku_idx]) == str(sku) and str(row[wh_idx]) == str(warehouse):
                row_idx, current_val = i, float(row[qty_idx]) if row[qty_idx] else 0.0
                break
        if row_idx > 0:
            ws.update_cell(row_idx, qty_idx + 1, current_val + delta_qty)
        else:
            ws.append_row([str(sku), warehouse, delta_qty])
    except Exception as e:
        st.error(f"庫存更新失敗: {e}")

def add_transaction(doc_type, date_str, sku, wh, qty, user, note):
    ws_hist = get_worksheet("History")
    prefix = {"進貨":"IN", "銷售出貨":"OUT", "製造領料":"MO", "製造入庫":"PD"}.get(doc_type, "ADJ")
    doc_no = f"{prefix}-{int(time.time())}"
    try:
        ws_hist.append_row([doc_type, doc_no, str(date_str), str(sku), wh, float(qty), user, note, 0, str(datetime.now())])
        factor = -1 if doc_type in ['銷售出貨', '製造領料', '庫存調整(減)'] else 1
        update_stock_qty(sku, wh, float(qty) * factor)
        clear_cache()
        return True
    except Exception as e:
        st.error(f"交易失敗: {e}")
        return False

def get_formatted_product_df():
    df = load_data("Products")
    if df.empty: return df
    df['label'] = df['sku'].astype(str) + " | " + df['name'].astype(str) + " (" + df['spec'].astype(str) + " / " + df['color'].astype(str) + ")"
    return df

# ==========================================
# 5. 主程式介面
# ==========================================
st.set_page_config(page_title=PAGE_TITLE, layout="wide", page_icon="💎")
st.title(f"💎 {PAGE_TITLE}")

if not get_client(): st.stop()

with st.sidebar:
    st.header("功能選單")
    page = st.radio("前往", ["📦 商品管理", "📦 移庫作業", "📥 進貨作業", "🚚 出貨作業", "🔨 製造作業", "⚖️ 庫存盤點", "📊 報表查詢"])
    if st.button("🔄 強制重新讀取"):
        clear_cache(); st.rerun()

# --- 📥 進貨作業 (修復空白問題) ---
if page == "📥 進貨作業":
    st.subheader("📥 進貨入庫")
    prods = get_formatted_product_df()
    if not prods.empty:
        with st.form("in_form"):
            c1, c2 = st.columns([2, 1])
            sel_p = c1.selectbox("選擇商品 (貨號|品名|規格)", prods['label'], index=0)
            wh = c2.selectbox("入庫倉庫", WAREHOUSES)
            c3, c4 = st.columns(2)
            qty = c3.number_input("數量", min_value=1.0, value=1.0)
            d_val = c4.date_input("日期", date.today())
            user = st.selectbox("經手人", KEYERS)
            note = st.text_input("備註")
            if st.form_submit_button("確認進貨"):
                if add_transaction("進貨", str(d_val), sel_p.split(" | ")[0], wh, qty, user, note):
                    st.success("成功！"); time.sleep(1); st.rerun()
    else:
        st.info("尚無商品資料。")

# --- 📦 商品管理 (修復 NameError) ---
elif page == "📦 商品管理":
    st.subheader("📦 商品資料維護")
    t1, t2 = st.tabs(["✨ 新增商品", "✏️ 修改/刪除商品"])
    with t1:
        c_cat, c_ser = st.columns(2)
        cat_opt = c_cat.selectbox("分類", CATEGORIES + ["➕ 手動..."])
        cat = c_cat.text_input("新分類") if "手動" in cat_opt else cat_opt
        ser_opt = c_ser.selectbox("系列", SERIES + ["➕ 手動..."])
        ser = c_ser.text_input("新系列") if "手動" in ser_opt else ser_opt
        
        current_df = load_data("Products")
        auto_sku = generate_auto_sku(ser, cat, set(current_df['sku'].astype(str)) if not current_df.empty else set())
        
        c1, c2 = st.columns(2)
        sku = c1.text_input("貨號", value=auto_sku)
        name = c2.text_input("品名 *必填")
        spec = st.text_input("規格")
        color = st.text_input("顏色")
        note = st.text_input("備註")
        
        if st.button("✨ 確認新增"):
            if sku and name:
                s, m = add_product(sku, name, cat, ser, spec, note, color)
                if s: st.success(m); time.sleep(1); st.rerun()
                else: st.error(m)
    with t2:
        df_prod = load_data("Products")
        if not df_prod.empty:
            sel_sku = st.selectbox("選擇修改商品", df_prod['sku'].astype(str))
            curr = df_prod[df_prod['sku'].astype(str) == sel_sku].iloc[0]
            with st.form("edit"):
                n_name = st.text_input("品名", curr['name'])
                n_spec = st.text_input("規格", curr['spec'])
                n_color = st.text_input("顏色", curr['color'])
                if st.form_submit_button("💾 儲存"):
                    s, m = update_product(sel_sku, {'name': n_name, 'spec': n_spec, 'color': n_color})
                    if s: st.success(m); time.sleep(1); st.rerun()
                    else: st.error(m)

# --- 其餘頁面 (移庫, 報表等) ---
elif page == "📊 報表查詢":
    st.subheader("📊 庫存報表")
    from main_logic import get_stock_overview # 假設你其餘邏輯在此
    df = get_stock_overview()
    st.dataframe(df, use_container_width=True)
