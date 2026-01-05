import streamlit as st
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime

# 1. 介面設定
st.set_page_config(
    page_title="GemCraft Cloud - 成本計算機",
    page_icon="☁️",
    layout="wide"
)

# --- ☁️ Google Sheets 設定區 ---
# 請將此處修改為你的 Google 試算表名稱 (顯示在網頁標題上的那個名稱)
SPREADSHEET_NAME = "GemCraft_Inventory_System" 
KEY_FILE = "google_key.json"

# 定義 Scope
SCOPES = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']

# --- 🔌 連線與資料處理函式 ---

@st.cache_resource
def get_google_sheet_client():
    """
    建立 Google Sheets 連線客戶端 (使用 cache_resource 避免重複連線)
    """
    try:
        creds = ServiceAccountCredentials.from_json_keyfile_name(KEY_FILE, SCOPES)
        client = gspread.authorize(creds)
        return client
    except Exception as e:
        st.error(f"❌ 無法連線至 Google API，請檢查金鑰檔案。\n錯誤訊息: {e}")
        return None

@st.cache_data(ttl=600)
def load_inventory_from_gsheet():
    """
    從 Google Sheets 讀取庫存資料 (設定 ttl=600，每 10 分鐘自動更新一次快取)
    假設庫存資料在 'sheet1' (第一個分頁)
    """
    client = get_google_sheet_client()
    if not client: return pd.DataFrame()

    try:
        # 開啟試算表
        sh = client.open(SPREADSHEET_NAME)
        # 讀取第一個分頁 (假設是庫存表)
        worksheet = sh.sheet1 
        data = worksheet.get_all_records()

        if not data:
            st.warning("⚠️ 雲端試算表是空的，請確認內容。")
            return pd.DataFrame(columns=['名稱', '規格', '平均成本'])

        df = pd.DataFrame(data)

        # 防呆：確保必要欄位存在
        required_cols = ['名稱', '規格']
        for col in required_cols:
            if col not in df.columns:
                df[col] = "未知"
        
        # 處理數值欄位
        if '平均成本' not in df.columns:
            df['平均成本'] = 0.0
        
        return df

    except gspread.exceptions.SpreadsheetNotFound:
        st.error(f"❌ 找不到名稱為 '{SPREADSHEET_NAME}' 的試算表，請確認名稱是否完全正確，且已分享給機器人信箱。")
        return pd.DataFrame()
    except Exception as e:
        st.error(f"❌ 讀取資料失敗: {e}")
        return pd.DataFrame()

def save_calculation_to_gsheet(record_dict):
    """
    將計算結果「附加 (Append)」到 Google Sheets 的 'history' 分頁
    """
    client = get_google_sheet_client()
    if not client: return

    try:
        sh = client.open(SPREADSHEET_NAME)
        
        # 嘗試取得名為 'history' 的分頁，若無則建立，或使用第二個分頁
        try:
            worksheet = sh.worksheet("history")
        except:
            # 若找不到 history 分頁，嘗試建立一個
            try:
                worksheet = sh.add_worksheet(title="history", rows="1000", cols="10")
                # 寫入標頭
                header = ["日期", "品名", "總成本", "建議售價(零)", "建議售價(批)", "材料明細"]
                worksheet.append_row(header)
            except:
                st.error("❌ 無法建立或找到 'history' 分頁，請手動在試算表中建立一個名為 history 的分頁。")
                return

        # 準備要寫入的一列資料
        row_data = [
            record_dict['日期'],
            record_dict['品名'],
            record_dict['總成本'],
            record_dict['零售價'],
            record_dict['批發價'],
            record_dict['材料明細']
        ]
        
        # 使用 append_row 將資料加到最後一行 (比整表覆蓋更安全且快)
        worksheet.append_row(row_data)
        st.toast("✅ 雲端存檔成功！", icon="☁️")
        
    except Exception as e:
        st.error(f"❌ 存檔失敗: {e}")

def init_session_state():
    if 'bom_list' not in st.session_state:
        st.session_state['bom_list'] = [] 

# --- 初始化 ---
init_session_state()

# 讀取雲端資料
with st.spinner('正在從 Google 雲端下載庫存資料...'):
    inventory_df = load_inventory_from_gsheet()

# --- Sidebar: 設定 ---
with st.sidebar:
    st.header("⚙️ 定價參數")
    st.markdown(f"連線狀態：{'🟢 線上' if not inventory_df.empty else '🔴 離線'}")
    
    if st.button("🔄 強制更新庫存資料"):
        load_inventory_from_gsheet.clear() # 清除快取
        st.rerun()

    st.markdown("---")
    retail_multiplier = st.number_input("🏷️ 零售倍率", value=2.5, step=0.1)
    wholesale_multiplier = st.number_input("📦 批發倍率", value=1.5, step=0.1)

# --- 主畫面 ---
st.title("💎 GemCraft Cloud 成本計算機")

tab_calc, tab_history = st.tabs(["💰 成本計算", "📜 雲端紀錄"])

with tab_calc:
    col_input, col_result = st.columns([1, 1], gap="large")
    
    # --- 左側：輸入區 ---
    with col_input:
        st.subheader("1️⃣ 選擇材料")
        
        if not inventory_df.empty:
            # 建立下拉選單字串
            inventory_df['display_name'] = inventory_df['名稱'].astype(str) + " (" + inventory_df['規格'].astype(str) + ")"
            options = inventory_df['display_name'].tolist()
        else:
            options = []
            
        selected_option = st.selectbox("選擇庫存材料", options=options, index=None)
        
        # 自動填入成本
        default_cost = 0.0
        if selected_option and not inventory_df.empty:
            row = inventory_df[inventory_df['display_name'] == selected_option].iloc[0]
            # 轉換為 float 避免錯誤
            try:
                default_cost = float(str(row.get('平均成本', 0)).replace(',', ''))
            except:
                default_cost = 0.0

        c1, c2, c3 = st.columns([2, 2, 1])
        with c1:
            input_cost = st.number_input("單價", value=default_cost, step=1.0, format="%.1f")
        with c2:
            input_qty = st.number_input("數量", min_value=1, value=1)
        with c3:
            add_btn = st.button("➕ 加入", use_container_width=True)
            
        if add_btn and selected_option:
            st.session_state['bom_list'].append({
                "項目": selected_option,
                "單價": input_cost,
                "數量": input_qty,
                "小計": input_cost * input_qty
            })

        # 顯示清單
        if st.session_state['bom_list']:
            st.dataframe(pd.DataFrame(st.session_state['bom_list']), hide_index=True, use_container_width=True)
            if st.button("🧹 清空"):
                st.session_state['bom_list'] = []
                st.rerun()
        
        st.markdown("---")
        st.markdown("##### 🛠️ 雜支設定")
        c_labor, c_pack = st.columns(2)
        with c_labor: labor_cost = st.number_input("工費", value=50.0)
        with c_pack: pack_cost = st.number_input("包材與運費", value=10.0)

    # --- 右側：結果與存檔 ---
    with col_result:
        st.subheader("2️⃣ 計算與存檔")
        
        material_total = sum(item['小計'] for item in st.session_state['bom_list'])
        total_cost = material_total + labor_cost + pack_cost
        
        final_retail = total_cost * retail_multiplier
        final_wholesale = total_cost * wholesale_multiplier
        
        st.metric("總成本", f"${total_cost:,.0f}")
        
        col_r, col_w = st.columns(2)
        with col_r: st.info(f"零售價: ${final_retail:,.0f}")
        with col_w: st.warning(f"批發價: ${final_wholesale:,.0f}")

        st.markdown("---")
        
        # 存檔區塊
        st.markdown("##### 💾 儲存此商品")
        product_name = st.text_input("輸入商品名稱 (例如：紫水晶手鍊-A款)")
        
        if st.button("☁️ 儲存至雲端紀錄表", type="primary", use_container_width=True):
            if not product_name:
                st.error("請輸入商品名稱！")
            elif total_cost == 0:
                st.error("成本為 0，無法儲存。")
            else:
                # 組合資料
                bom_str = ", ".join([f"{x['項目']}x{x['數量']}" for x in st.session_state['bom_list']])
                record = {
                    "日期": datetime.now().strftime("%Y-%m-%d %H:%M"),
                    "品名": product_name,
                    "總成本": total_cost,
                    "零售價": final_retail,
                    "批發價": final_wholesale,
                    "材料明細": bom_str
                }
                
                with st.spinner("正在寫入 Google Sheets..."):
                    save_calculation_to_gsheet(record)

with tab_history:
    st.info("此處可擴充功能：讀取 'history' 分頁的資料並顯示為表格。")
    if st.button("讀取雲端歷史紀錄"):
        client = get_google_sheet_client()
        if client:
            try:
                # 嘗試讀取 history 分頁
                history_df = pd.DataFrame(client.open(SPREADSHEET_NAME).worksheet("history").get_all_records())
                st.dataframe(history_df)
            except:
                st.warning("尚無歷史紀錄或找不到 history 分頁。")
