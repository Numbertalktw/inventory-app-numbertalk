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
        # 穩定性強化：確保欄位完整並填充空值
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

def get_formatted_product_df():
    """統一商品選單顯示格式"""
    df = load_data("Products")
    if df.empty: return df
    df['label'] = (
        df['sku'].astype(str) + " | " + 
        df['name'].astype(str) + 
        " (" + df['spec'].astype(str) + " / " + df['color'].astype(str) + ")"
    )
    return df

def generate_auto_sku(series, category, existing_skus_set):
    prefix = PREFIX_MAP.get(series, PREFIX_MAP.get(category, "XX"))
    count = 1
    while True:
        candidate = f"{prefix}-{count:03d}" 
        if candidate not in existing_skus_set: return candidate
        count += 1
        if count > 999: return f"{prefix}-{int(time.time())}"

def add_product(sku, name, category, series, spec, note, color):
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

def add_transaction(doc_type, date_str, sku, wh, qty, user, note, cost=0):
    ws_hist = get_worksheet("History")
    prefix = {"進貨":"IN", "銷售出貨":"OUT", "製造領料":"MO", "製造入庫":"PD"}.get(doc_type, "ADJ")
    doc_no = f"{prefix}-{int(time.time())}"
    try:
        ws_hist.append_row([doc_type, doc_no, str(date_str), str(sku), wh, float(qty), user, note, float(cost), str(datetime.now())])
        factor = -1 if doc_type in ['銷售出貨', '製造領料', '庫存調整(減)'] else 1
        update_stock_qty(sku, wh, float(qty) * factor)
        clear_cache()
        return True
    except Exception as e:
        st.error(f"交易記錄失敗: {e}")
        return False

def render_history_table(doc_type_filter=None):
    st.markdown("#### 🕒 最近紀錄 (可刪除)")
    df = load_data("History")
    if df.empty:
        st.info("尚無紀錄"); return
    df_prod = load_data("Products")
    sku_map = dict(zip(df_prod['sku'].astype(str), df_prod['name'])) if not df_prod.empty else {}
    if doc_type_filter:
        df = df[df['doc_type'].isin(doc_type_filter)] if isinstance(doc_type_filter, list) else df[df['doc_type'] == doc_type_filter]
    
    df = df.sort_index(ascending=False).head(10)
    cols = st.columns([1.5, 1.5, 3, 1, 1, 1, 2])
    for col, h in zip(cols, ["單號", "日期", "品名 / SKU", "倉庫", "數量", "經手", "備註"]): col.markdown(f"**{h}**")
    for idx, row in df.iterrows():
        c1, c2, c3, c4, c5, c6, c7 = st.columns([1.5, 1.5, 3, 1, 1, 1, 2])
        c1.text(row.get('doc_no', '')[-10:])
        c2.text(row.get('date', ''))
        sku = str(row.get('sku',''))
        c3.text(f"{sku_map.get(sku, '未知')}\n({sku})")
        c4.text(row.get('warehouse', ''))
        c5.text(row.get('qty', 0))
        c6.text(row.get('user', ''))
        c7.text(row.get('note', ''))
        st.divider()

# ==========================================
# 4. 主程式介面
# ==========================================
st.set_page_config(page_title=PAGE_TITLE, layout="wide", page_icon="💎")
st.title(f"💎 {PAGE_TITLE}")

with st.sidebar:
    st.header("功能選單")
    page = st.radio("前往", ["📦 商品管理", "📦 移庫作業", "📥 進貨作業", "🚚 出貨作業", "🔨 製造作業", "📊 報表查詢"])
    if st.button("🔄 強制重新讀取"):
        clear_cache(); st.rerun()

# --- 📥 進貨作業 ---
if page == "📥 進貨作業":
    st.subheader("📥 進貨入庫")
    prods = get_formatted_product_df()
    if not prods.empty:
        with st.form("in_form"):
            c1, c2 = st.columns([2, 1])
            sel_p = c1.selectbox("選擇商品", prods['label'], index=0)
            wh = c2.selectbox("入庫倉庫", WAREHOUSES)
            c3, c4, c5 = st.columns(3)
            qty = c3.number_input("進貨數量", min_value=1.0, value=1.0)
            total_cost = c4.number_input("💰 進貨總價", min_value=0.0, value=0.0)
            d_val = c5.date_input("日期", date.today())
            user = st.selectbox("經手人", KEYERS)
            note = st.text_input("進貨備註")
            if st.form_submit_button("執行進貨"):
                if add_transaction("進貨", str(d_val), sel_p.split(" | ")[0], wh, qty, user, note, cost=total_cost):
                    st.success("✅ 進貨成功！"); time.sleep(1); st.rerun()
    else:
        st.info("尚無商品資料。")
    render_history_table("進貨")

# --- 🚚 出貨作業 ---
elif page == "🚚 出貨作業":
    st.subheader("🚚 銷售出貨")
    prods = get_formatted_product_df()
    if not prods.empty:
        with st.form("out_form"):
            c1, c2 = st.columns([2, 1])
            sel_p = c1.selectbox("選擇出貨商品", prods['label'], index=0)
            wh = c2.selectbox("出貨倉庫", WAREHOUSES, index=2)
            c3, c4 = st.columns(2)
            qty = c3.number_input("出貨數量", min_value=1.0, value=1.0)
            d_val = c4.date_input("日期", date.today())
            user = st.selectbox("經手人", KEYERS)
            note = st.text_input("訂單編號 / 備註")
            if st.form_submit_button("執行出貨"):
                if add_transaction("銷售出貨", str(d_val), sel_p.split(" | ")[0], wh, qty, user, note):
                    st.success("✅ 出貨成功！"); time.sleep(1); st.rerun()
    else:
        st.info("尚無商品資料。")
    render_history_table("銷售出貨")

# --- 其餘分頁邏輯在此處繼續 ... ---
elif page == "📦 商品管理":
    st.subheader("📦 商品資料維護")
    # 此處保留你原本的新增/修改邏輯，確保 add_product 被定義
    pass

elif page == "📊 報表查詢":
    st.subheader("📊 庫存報表")
    # 報表邏輯
    pass
