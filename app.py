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
        return gspread.authorize(creds)
    except:
        return None

def get_worksheet(sheet_name):
    client = get_client()
    if not client: return None
    try:
        return client.open(SPREADSHEET_NAME).worksheet(sheet_name)
    except:
        return None

@st.cache_data(ttl=5) 
def load_data(sheet_name):
    ws = get_worksheet(sheet_name)
    if ws is None: return pd.DataFrame()
    try:
        data = ws.get_all_records()
        df = pd.DataFrame(data)
        # 補齊可能缺失的欄位
        for col in ['sku', 'name', 'category', 'series', 'spec', 'color', 'note']:
            if col not in df.columns: df[col] = ""
        return df.fillna("")
    except:
        return pd.DataFrame()

def clear_cache():
    load_data.clear()

# ==========================================
# 3. 核心邏輯功能
# ==========================================

def get_formatted_product_df():
    df = load_data("Products")
    if df.empty: return df
    df['sku'] = df['sku'].astype(str)
    df['name'] = df['name'].astype(str)
    df['spec'] = df['spec'].astype(str)
    df['color'] = df['color'].astype(str)
    df['label'] = df['sku'] + " | " + df['name'] + " (" + df['spec'] + " / " + df['color'] + ")"
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
    except:
        return False

def add_product(sku, name, category, series, spec, note, color):
    ws = get_worksheet("Products")
    try:
        ws.append_row([str(sku), series, category, name, spec, color, note])
        clear_cache()
        return True
    except:
        return False

# ==========================================
# 4. 主程式介面
# ==========================================
st.set_page_config(page_title=PAGE_TITLE, layout="wide", page_icon="💎")

# 強制淺色模式的 CSS 注入 (選用)
st.markdown("""
    <style>
    .stApp { background-color: white; }
    </style>
    """, unsafe_allow_html=True)

st.title(f"💎 {PAGE_TITLE}")

# 側邊欄選單
with st.sidebar:
    st.header("功能選單")
    page = st.radio("前往", ["📦 商品管理", "🚚 出貨作業", "📊 報表查詢"])
    if st.button("🔄 刷新資料"):
        clear_cache()
        st.rerun()

# --- 📦 商品管理 ---
if page == "📦 商品管理":
    st.subheader("📦 商品資料維護")
    t1, t2 = st.tabs(["✨ 新增商品", "✏️ 修改商品"])
    
    with t1:
        st.markdown("#### 新增商品資料")
        c1, c2 = st.columns(2)
        new_sku = c1.text_input("貨號 (SKU)")
        new_name = c2.text_input("品名")
        new_cat = st.selectbox("分類", CATEGORIES)
        new_ser = st.selectbox("系列", SERIES)
        new_spec = st.text_input("規格")
        new_color = st.text_input("顏色")
        new_note = st.text_area("備註")
        
        if st.button("✅ 確認新增"):
            if new_sku and new_name:
                if add_product(new_sku, new_name, new_cat, new_ser, new_spec, new_note, new_color):
                    st.success("商品新增成功！")
                    time.sleep(1)
                    st.rerun()
            else:
                st.error("請填寫貨號與品名")

    with t2:
        st.markdown("#### 修改現有商品")
        df_p = get_formatted_product_df()
        
        # ✨ 重點：加入防報錯檢查
        if not df_p.empty:
            all_labels = df_p['label'].tolist()
            sel_label = st.selectbox("🔍 搜尋並選擇商品", options=all_labels)
            
            if sel_label:
                sku = sel_label.split(" | ")[0]
                curr = df_p[df_p['sku'] == sku].iloc[0]
                
                with st.form("edit_form"):
                    st.info(f"正在編輯：{sku}")
                    edit_name = st.text_input("品名", value=str(curr['name']))
                    edit_spec = st.text_input("規格", value=str(curr['spec']))
                    edit_color = st.text_input("顏色", value=str(curr['color']))
                    edit_note = st.text_area("備註", value=str(curr['note']))
                    
                    if st.form_submit_button("💾 儲存修改"):
                        if update_product(sku, {
                            'name': edit_name, 
                            'spec': edit_spec, 
                            'color': edit_color, 
                            'note': edit_note
                        }):
                            st.success("資料已更新！")
                            time.sleep(1)
                            st.rerun()
        else:
            st.warning("⚠️ 目前資料庫中沒有商品，請先到「新增商品」分頁建立資料。")

# --- 📊 報表查詢 ---
elif page == "📊 報表查詢":
    st.subheader("📊 庫存清單總覽")
    df = load_data("Products")
    if not df.empty:
        st.dataframe(df, use_container_width=True)
    else:
        st.info("暫無資料")

# --- 🚚 出貨作業 (範例結構) ---
elif page == "🚚 出貨作業":
    st.subheader("🚚 銷售出貨")
    st.info("此功能可依照您的需求繼續擴充清單邏輯...")
