import streamlit as st
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import date, datetime
import time

# ==========================================
# 1. 系統基礎設定
# ==========================================
PAGE_TITLE = "numbertalk 雲端庫存系統"
SPREADSHEET_NAME = "numbertalk-system" 

WAREHOUSES = ["Wen", "千畇", "James", "Imeng"]
CATEGORIES = ["天然石", "金屬配件", "線材", "包裝材料", "完成品", "數字珠", "數字串", "香料", "手作設備"]
SERIES = ["原料", "半成品", "成品", "包材", "生命數字能量項鍊", "數字手鍊", "貼紙", "小卡", "火漆章", "能量蠟燭", "香包", "水晶", "魔法鹽"]
KEYERS = ["Wen", "千畇", "James", "Imeng", "小幫手"]

# ==========================================
# 2. Google Sheet 連線與資料讀取
# ==========================================
SCOPE = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']

@st.cache_resource
def get_client():
    try:
        creds_dict = st.secrets["gcp_service_account"]
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, SCOPE)
        return gspread.authorize(creds)
    except: return None

def get_worksheet(sheet_name):
    client = get_client()
    try: return client.open(SPREADSHEET_NAME).worksheet(sheet_name)
    except: return None

@st.cache_data(ttl=5) 
def load_data(sheet_name):
    ws = get_worksheet(sheet_name)
    if ws:
        df = pd.DataFrame(ws.get_all_records())
        return df.fillna("")
    return pd.DataFrame()

def clear_cache(): load_data.clear()

# ==========================================
# 3. 核心邏輯
# ==========================================

def get_formatted_product_df():
    df = load_data("Products")
    if df.empty: return df
    df['sku'] = df['sku'].astype(str)
    df['name'] = df['name'].astype(str)
    df['label'] = df['sku'] + " | " + df['name'] + " (" + df['spec'].astype(str) + " / " + df['color'].astype(str) + ")"
    return df

def update_product(sku, new_data):
    ws = get_worksheet("Products")
    try:
        cell = ws.find(str(sku))
        row = cell.row
        if 'name' in new_data: ws.update_cell(row, 4, new_data['name'])
        if 'spec' in new_data: ws.update_cell(row, 5, new_data['spec'])
        if 'color' in new_data: ws.update_cell(row, 6, new_data['color'])
        if 'note' in new_data: ws.update_cell(row, 7, new_data['note'])
        clear_cache()
        return True
    except: return False

def add_transaction(doc_type, date_str, sku, wh, qty, user, note):
    ws_hist = get_worksheet("History")
    doc_no = f"TR-{int(time.time())}"
    ws_hist.append_row([doc_type, doc_no, str(date_str), str(sku), wh, float(qty), user, note, 0, str(datetime.now())])
    clear_cache()
    return True

# ==========================================
# 4. 介面
# ==========================================
st.set_page_config(page_title=PAGE_TITLE, layout="wide")
st.title(PAGE_TITLE)

page = st.sidebar.radio("功能選單", ["📦 商品管理", "🚚 出貨作業", "🔨 製造作業", "📊 報表查詢"])

if page == "📦 商品管理":
    t1, t2 = st.tabs(["✨ 新增", "✏️ 修改"])
    with t2:
        df_p = get_formatted_product_df()
        if not df_p.empty:
            sel_label = st.selectbox("🔍 選擇商品", df_p['label'].tolist())
            sel_sku = sel_label.split(" | ")[0]
            curr = df_p[df_p['sku'] == sel_sku].iloc[0]
            with st.form("edit"):
                n_name = st.text_input("品名", value=curr['name'])
                n_spec = st.text_input("規格", value=curr['spec'])
                n_color = st.text_input("顏色", value=curr['color'])
                n_note = st.text_input("備註", value=curr['note'])
                if st.form_submit_button("💾 儲存修改"):
                    update_product(sel_sku, {'name': n_name, 'spec': n_spec, 'color': n_color, 'note': n_note})
                    st.success("更新成功"); time.sleep(1); st.rerun()

elif page == "🚚 出貨作業":
    st.subheader("🚚 出貨 (多品項清單)")
    if 'out_list' not in st.session_state: st.session_state['out_list'] = []
    
    order_info = st.text_input("客戶/訂單號碼")
    prods = get_formatted_product_df()
    c1, c2, c3 = st.columns([3,1,1])
    sel_p = c1.selectbox("商品", prods['label'])
    wh = c2.selectbox("倉庫", WAREHOUSES)
    qty = c3.number_input("數量", 1.0)
    
    if st.button("⬇️ 加入清單"):
        st.session_state['out_list'].append({'sku': sel_p.split(" | ")[0], 'name': sel_p.split(" | ")[1], 'wh': wh, 'qty': qty})
        st.rerun()
        
    for i, item in enumerate(st.session_state['out_list']):
        st.write(f"🔸 {item['name']} x{item['qty']} ({item['wh']})")
        
    if st.button("✅ 確認出貨") and st.session_state['out_list']:
        for x in st.session_state['out_list']:
            add_transaction("銷售出貨", date.today(), x['sku'], x['wh'], x['qty'], "系統", order_info)
        st.session_state['out_list'] = []; st.success("成功"); time.sleep(1); st.rerun()

elif page == "📊 報表查詢":
    st.dataframe(load_data("Products"))
