import streamlit as st
import pandas as pd
from datetime import date, datetime
import os
import time

# ==========================================
# 1. 系統設定
# ==========================================

PAGE_TITLE = "商品庫存 & 出貨管理系統 (自動拆分版)"
INVENTORY_FILE = 'inventory_data_auto.csv'
HISTORY_FILE = 'history_data_auto.csv'

# --- 出貨表 (流水帳) 18 欄位 ---
HISTORY_COLUMNS = [
    '單號', '日期', '系列', '分類', '品名', '貨號', 
    '出庫單號(可複寫)', '出入庫', '數量', '經手人', 
    '訂單單號', '出貨日期', '貨號備註', '運費', 
    '款項結清', '工資', '發票', '備註'
]

# --- 庫存表 (系統自動計算) ---
INVENTORY_COLUMNS = [
    '貨號', '系列', '分類', '品名', 
    '庫存數量', '平均成本'
]

DEFAULT_SERIES = ["生命數字能量項鍊", "一般款", "客製化", "福利品"]
DEFAULT_CATEGORIES = ["包裝材料", "天然石", "配件", "耗材", "成品"]
DEFAULT_HANDLERS = ["Wen", "店長", "小幫手"]

# ==========================================
# 2. 核心函式
# ==========================================

def load_data():
    """讀取資料"""
    if os.path.exists(INVENTORY_FILE):
        try:
            inv_df = pd.read_csv(INVENTORY_FILE)
            inv_df['貨號'] = inv_df['貨號'].astype(str)
        except:
            inv_df = pd.DataFrame(columns=INVENTORY_COLUMNS)
    else:
        inv_df = pd.DataFrame(columns=INVENTORY_COLUMNS)

    if os.path.exists(HISTORY_FILE):
        try:
            hist_df = pd.read_csv(HISTORY_FILE)
            for col in HISTORY_COLUMNS:
                if col not in hist_df.columns:
                    hist_df[col] = ""
            hist_df = hist_df[HISTORY_COLUMNS]
        except:
            hist_df = pd.DataFrame(columns=HISTORY_COLUMNS)
    else:
        hist_df = pd.DataFrame(columns=HISTORY_COLUMNS)
        
    return inv_df, hist_df

def save_data():
    """存檔"""
    if 'inventory' in st.session_state:
        st.session_state['inventory'].to_csv(INVENTORY_FILE, index=False, encoding='utf-8-sig')
    if 'history' in st.session_state:
        st.session_state['history'].to_csv(HISTORY_FILE, index=False, encoding='utf-8-sig')

def process_master_upload(file_obj):
    """
    核心邏輯：
    1. 讀取總表 -> 成為 History
    2. 根據 '貨號' 與 '出入庫' -> 計算 Inventory
    """
    try:
        # 1. 讀取檔案
        if file_obj.name.endswith('.csv'):
            df = pd.read_csv(file_obj)
        else:
            df = pd.read_excel(file_obj)
        
        # 欄位對應容錯
        rename_map = {
            '出庫單號': '出庫單號(可複寫)',
            '商品貨號': '貨號', '商品品名': '品名', '商品系列': '系列', '商品分類': '分類'
        }
        df = df.rename(columns=rename_map)

        # 補齊 18 欄位
        for col in HISTORY_COLUMNS:
            if col not in df.columns:
                df[col] = ""
        
        # 轉型與處理空值
        df['單號'] = df['單號'].astype(str)
        df['貨號'] = df['貨號'].astype(str)
        df['數量'] = pd.to_numeric(df['數量'], errors='coerce').fillna(0)
        
        # --- 產生 History Table ---
        history_df = df[HISTORY_COLUMNS].copy()

        # --- 自動計算 Inventory Table ---
        # 取得所有唯一的商品資訊
        inventory_items = df[['貨號', '系列', '分類', '品名']].drop_duplicates(subset=['貨號']).copy()
        
        # 計算庫存數量
        # 邏輯：如果 '出入庫' 欄位包含 "入庫" 則加，包含 "出庫" 則減
        inventory_items['庫存數量'] = 0.0
        inventory_items['平均成本'] = 0.0 # 若 Excel 沒提供成本，暫設為 0

        for idx, row in inventory_items.iterrows():
            sku = row['貨號']
            # 找出該貨號的所有紀錄
            item_hist = df[df['貨號'] == sku]
            
            total_stock = 0
            for _, h_row in item_hist.iterrows():
                qty = h_row['數量']
                action = str(h_row['出入庫'])
                
                if "入庫" in action:
                    total_stock += qty
                elif "出庫" in action:
                    total_stock -= qty
            
            inventory_items.at[idx, '庫存數量'] = total_stock

        return history_df, inventory_items[INVENTORY_COLUMNS]

    except Exception as e:
        st.error(f"檔案解析失敗: {e}")
        return None, None

# ==========================================
# 3. 初始化
# ==========================================

if 'inventory' not in st.session_state:
    inv, hist = load_data()
    st.session_state['inventory'] = inv
    st.session_state['history'] = hist

# ==========================================
# 4. 介面
# ==========================================

st.set_page_config(page_title=PAGE_TITLE, layout="wide", page_icon="🏢")
st.title(f"🏢 {PAGE_TITLE}")

# --- 側邊欄 ---
with st.sidebar:
    st.header("功能導航")
    page = st.radio("前往", ["📤 上傳總表 (自動拆分)", "🚚 查看出貨表 (歷史)", "📊 查看庫存表 (狀態)", "➕ 新增單據"])
    
    st.divider()
    st.header("💾 資料匯出")
    if not st.session_state['inventory'].empty:
        csv_i = st.session_state['inventory'].to_csv(index=False).encode('utf-8-sig')
        st.download_button("📥 下載【庫存表】", csv_i, f'Stock_{date.today()}.csv', "text/csv")
        
    if not st.session_state['history'].empty:
        csv_h = st.session_state['history'].to_csv(index=False).encode('utf-8-sig')
        st.download_button("📥 下載【出貨表/總表】", csv_h, f'Master_History_{date.today()}.csv', "text/csv")

# ---------------------------------------------------------
# 頁面 1: 上傳總表 (核心功能)
# ---------------------------------------------------------
if page == "📤 上傳總表 (自動拆分)":
    st.subheader("📤 上傳總表 (Master File)")
    st.info("請上傳您的 Excel 總表 (含18個欄位)。系統將自動儲存紀錄，並幫您算出庫存量。")
    
    uploaded_file = st.file_uploader("選擇檔案 (.xlsx / .csv)", type=['xlsx', 'xls', 'csv'])
    
    if uploaded_file is not None:
        if st.button("🚀 開始拆分並匯入", type="primary"):
            hist_df, inv_df = process_master_upload(uploaded_file)
            
            if hist_df is not None and inv_df is not None:
                st.session_state['history'] = hist_df
                st.session_state['inventory'] = inv_df
                save_data()
                
                st.success(f"✅ 成功！已匯入 {len(hist_df)} 筆交易紀錄。")
                st.success(f"✅ 自動計算出 {len(inv_df)} 項商品的庫存數量。")
                time.sleep(1)

# ---------------------------------------------------------
# 頁面 2: 出貨表
# ---------------------------------------------------------
elif page == "🚚 查看出貨表 (歷史)":
    st.subheader("🚚 商品出貨表 (流水帳)")
    df = st.session_state['history']
    
    if df.empty:
        st.warning("目前無資料。請先到「上傳總表」匯入 Excel。")
    else:
        search = st.text_input("🔍 搜尋紀錄", "")
        if search:
            mask = df.astype(str).apply(lambda x: x.str.contains(search, case=False)).any(axis=1)
            df = df[mask]
            
        edited_df = st.data_editor(df, use_container_width=True, num_rows="dynamic", height=600)
        if st.button("💾 儲存修改"):
            st.session_state['history'] = edited_df
            save_data()
            st.success("已更新")

# ---------------------------------------------------------
# 頁面 3: 庫存表
# ---------------------------------------------------------
elif page == "📊 查看庫存表 (狀態)":
    st.subheader("📊 商品庫存表 (系統自動計算)")
    st.caption("此表是根據「出貨表」的 入庫-出庫 自動計算出來的結果。")
    
    df = st.session_state['inventory']
    if df.empty:
        st.warning("目前無庫存資料。")
    else:
        st.dataframe(
            df, 
            use_container_width=True,
            column_config={
                "庫存數量": st.column_config.NumberColumn(format="%d"),
                "平均成本": st.column_config.NumberColumn(format="$%.2f")
            }
        )

# ---------------------------------------------------------
# 頁面 4: 新增單據
# ---------------------------------------------------------
elif page == "➕ 新增單據":
    st.subheader("➕ 新增單據")
    st.caption("新增後會同時寫入出貨表，並更新庫存表。")
    
    inv_df = st.session_state['inventory']
    if inv_df.empty:
        st.warning("請先上傳總表建立基礎資料。")
    else:
        inv_df['label'] = inv_df['貨號'] + " | " + inv_df['品名'] + " | 剩餘:" + inv_df['庫存數量'].astype(str)
        c1, c2 = st.columns([2, 1])
        with c1:
            sel = st.selectbox("選擇商品", inv_df['label'].tolist())
            row = inv_df[inv_df['label'] == sel].iloc[0]
            idx = inv_df[inv_df['label'] == sel].index[0]
        with c2:
            act = st.radio("動作", ["入庫", "出庫"], horizontal=True)
            
        st.divider()
        with st.form("entry"):
            # 18欄輸入介面
            r1c1, r1c2, r1c3 = st.columns(3)
            date_val = r1c1.date_input("日期", date.today())
            qty_val = r1c2.number_input("數量", 1)
            hand_val = r1c3.selectbox("經手人", DEFAULT_HANDLERS)
            
            r2c1, r2c2, r2c3, r2c4 = st.columns(4)
            ord_id = r2c1.text_input("訂單單號")
            ship_d = r2c2.date_input("出貨日期", date.today())
            out_id = r2c3.text_input("出庫單號")
            sku_nt = r2c4.text_input("貨號備註")
            
            r3c1, r3c2, r3c3, r3c4 = st.columns(4)
            fee = r3c1.text_input("運費")
            pay = r3c2.selectbox("結清", ["", "是", "否"])
            lab = r3c3.text_input("工資")
            inv_n = r3c4.text_input("發票")
            note = st.text_area("備註")

            if st.form_submit_button("✅ 確認"):
                # 更新庫存
                curr = float(row['庫存數量'])
                if act == "入庫": st.session_state['inventory'].at[idx, '庫存數量'] = curr + qty_val
                else: st.session_state['inventory'].at[idx, '庫存數量'] = curr - qty_val
                
                # 寫入歷史
                rec = {
                    '單號': datetime.now().strftime('%Y%m%d%H%M%S'),
                    '日期': date_val,
                    '系列': row['系列'], '分類': row['分類'], '品名': row['品名'], '貨號': row['貨號'],
                    '出庫單號(可複寫)': out_id, '出入庫': f"{act}-{hand_val}",
                    '數量': qty_val, '經手人': hand_val,
                    '訂單單號': ord_id, '出貨日期': ship_d if act == '出庫' else None,
                    '貨號備註': sku_nt, '運費': fee,
                    '款項結清': pay, '工資': lab, '發票': inv_n, '備註': note
                }
                st.session_state['history'] = pd.concat([st.session_state['history'], pd.DataFrame([rec])], ignore_index=True)
                save_data()
                st.success("成功！")
                time.sleep(1)
                st.rerun()
