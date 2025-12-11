import streamlit as st
import pandas as pd
from datetime import date, datetime
import os
import time

# ==========================================
# 1. 系統設定
# ==========================================

PAGE_TITLE = "商品庫存管理系統 (Excel上傳版)"
INVENTORY_FILE = 'inventory_data_v3.csv'
HISTORY_FILE = 'history_data_excel_v3.csv'

# 歷史紀錄欄位 (18欄)
HISTORY_COLUMNS = [
    '單號', '日期', '系列', '分類', '品名', '貨號', 
    '出庫單號(可複寫)', '出入庫', '數量', '經手人', 
    '訂單單號', '出貨日期', '貨號備註', '運費', 
    '款項結清', '工資', '發票', '備註'
]

# 庫存檔欄位 (您的 Excel 需要有這些標題)
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
    # 庫存
    if os.path.exists(INVENTORY_FILE):
        try:
            inv_df = pd.read_csv(INVENTORY_FILE)
            for col in INVENTORY_COLUMNS:
                if col not in inv_df.columns:
                    inv_df[col] = 0 if '數量' in col or '成本' in col else ""
            inv_df['貨號'] = inv_df['貨號'].astype(str)
        except:
            inv_df = pd.DataFrame(columns=INVENTORY_COLUMNS)
    else:
        inv_df = pd.DataFrame(columns=INVENTORY_COLUMNS)

    # 紀錄
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

def generate_sku(category, df):
    prefix_map = {'天然石': 'ST', '配件': 'AC', '耗材': 'OT', '包裝材料': 'PK', '成品': 'PD'}
    prefix = prefix_map.get(category, "XX")
    if df.empty: return f"{prefix}0001"
    mask = df['貨號'].astype(str).str.startswith(prefix)
    existing = df.loc[mask, '貨號']
    if existing.empty: return f"{prefix}0001"
    try:
        max_num = existing.str.extract(r'(\d+)')[0].astype(float).max()
        return f"{prefix}{int(max_num)+1:04d}"
    except:
        return f"{prefix}{int(time.time())}"

def get_options(df, col, default):
    opts = set(default)
    if not df.empty and col in df.columns:
        exist = df[col].dropna().unique().tolist()
        opts.update([str(x) for x in exist if str(x).strip()])
    return ["➕ 手動輸入"] + sorted(list(opts))

def process_excel_upload(file_obj):
    """處理上傳的 Excel"""
    try:
        if file_obj.name.endswith('.csv'):
            df = pd.read_csv(file_obj)
        else:
            df = pd.read_excel(file_obj)
        
        # 欄位對應檢查與標準化
        # 這裡做一個簡單的對應，防止使用者欄位名稱不同
        # 如果使用者上傳的欄位包含 '品名' 或 '名稱' -> 對應到 '品名'
        col_map = {
            '名稱': '品名', '商品名稱': '品名',
            '數量': '庫存數量', '庫存': '庫存數量',
            '成本': '平均成本', '單價': '平均成本',
            '類別': '分類', '商品分類': '分類'
        }
        df = df.rename(columns=col_map)
        
        # 補齊系統需要的欄位
        for col in INVENTORY_COLUMNS:
            if col not in df.columns:
                if col == '貨號': # 如果沒貨號，自動產生太複雜，先給空值
                     df[col] = [f"AUTO-{i}" for i in range(len(df))]
                elif col == '平均成本' or col == '庫存數量':
                    df[col] = 0
                else:
                    df[col] = ""
                    
        # 強制轉型
        df['貨號'] = df['貨號'].astype(str)
        df['庫存數量'] = pd.to_numeric(df['庫存數量'], errors='coerce').fillna(0)
        df['平均成本'] = pd.to_numeric(df['平均成本'], errors='coerce').fillna(0)
        
        return df[INVENTORY_COLUMNS]
    except Exception as e:
        st.error(f"檔案解析失敗: {e}")
        return None

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
    page = st.radio("前往", ["📝 庫存異動", "📦 商品建檔與庫存表", "📜 歷史紀錄"])
    
    st.divider()
    st.header("💾 資料管理")
    
    # 上傳區 (側邊欄常駐)
    with st.expander("📤 上傳 Excel 匯入/覆蓋", expanded=True):
        st.caption("請上傳包含 `品名`, `分類`, `庫存數量` 等欄位的 Excel。")
        up_file = st.file_uploader("選擇檔案 (.xlsx/.csv)", type=['xlsx', 'xls', 'csv'], key="sidebar_up")
        if up_file and st.button("確認覆蓋庫存", key="sidebar_btn"):
            new_df = process_excel_upload(up_file)
            if new_df is not None:
                st.session_state['inventory'] = new_df
                save_data()
                st.success(f"成功匯入 {len(new_df)} 筆資料！")
                time.sleep(1)
                st.rerun()

    # 下載區
    if not st.session_state['inventory'].empty:
        st.divider()
        csv = st.session_state['inventory'].to_csv(index=False).encode('utf-8-sig')
        st.download_button("📥 下載庫存表", csv, f'Inventory_{date.today()}.csv', "text/csv")

# ---------------------------------------------------------
# 頁面內容
# ---------------------------------------------------------

if page == "📝 庫存異動":
    st.subheader("📝 庫存異動 (入庫/出庫)")
    inv_df = st.session_state['inventory']
    
    if inv_df.empty:
        st.warning("⚠️ 目前無資料，請先上傳 Excel 或建立商品。")
    else:
        # 選商品
        inv_df['label'] = inv_df['貨號'] + " | " + inv_df['品名'] + " | 庫存:" + inv_df['庫存數量'].astype(str)
        c1, c2 = st.columns([2, 1])
        with c1:
            sel_label = st.selectbox("選擇商品", inv_df['label'].tolist())
            row = inv_df[inv_df['label'] == sel_label].iloc[0]
            idx = inv_df[inv_df['label'] == sel_label].index[0]
        with c2:
            act = st.radio("動作", ["入庫", "出庫"], horizontal=True)
            
        st.divider()
        
        with st.form("act_form"):
            st.markdown(f"正在操作：**{row['品名']}** ({row['貨號']})")
            
            # 18欄位輸入區
            col1, col2, col3, col4 = st.columns(4)
            d_date = col1.date_input("日期", value=date.today())
            d_qty = col2.number_input("數量", 1)
            d_handler = col3.selectbox("經手人", DEFAULT_HANDLERS)
            d_out_id = col4.text_input("出庫單號 (選填)")
            
            col5, col6, col7, col8 = st.columns(4)
            d_order = col5.text_input("訂單單號")
            d_ship = col6.date_input("出貨日期", value=date.today())
            d_snote = col7.text_input("貨號備註")
            d_fee = col8.text_input("運費")
            
            col9, col10, col11, col12 = st.columns(4)
            d_pay = col9.selectbox("款項結清", ["", "是", "否"])
            d_labor = col10.text_input("工資")
            d_inv = col11.text_input("發票")
            
            d_note = st.text_area("備註")
            
            # 入庫成本
            cost_in = 0
            if act == "入庫":
                cost_in = st.number_input("本次進貨總成本 (計算平均成本用)", min_value=0)

            if st.form_submit_button("✅ 送出"):
                # 庫存邏輯
                curr_q = float(row['庫存數量'])
                curr_c = float(row['平均成本'])
                
                if act == "入庫":
                    new_q = curr_q + d_qty
                    new_c = ((curr_q * curr_c) + cost_in) / new_q if new_q > 0 else 0
                    st.session_state['inventory'].at[idx, '庫存數量'] = new_q
                    st.session_state['inventory'].at[idx, '平均成本'] = new_c
                    st.success(f"已入庫 {d_qty} 個")
                else:
                    new_q = curr_q - d_qty
                    st.session_state['inventory'].at[idx, '庫存數量'] = new_q
                    st.success(f"已出庫 {d_qty} 個")
                
                # 紀錄邏輯 (18欄)
                rec = {
                    '單號': datetime.now().strftime('%Y%m%d%H%M%S'),
                    '日期': d_date,
                    '系列': row['系列'], '分類': row['分類'], '品名': row['品名'], '貨號': row['貨號'],
                    '出庫單號(可複寫)': d_out_id if d_out_id else (f"OUT-{date.today()}" if act == '出庫' else ''),
                    '出入庫': f"{act}-{d_handler}",
                    '數量': d_qty, '經手人': d_handler,
                    '訂單單號': d_order, '出貨日期': d_ship if act == '出庫' else None,
                    '貨號備註': d_snote, '運費': d_fee, '款項結清': d_pay,
                    '工資': d_labor, '發票': d_inv, '備註': d_note
                }
                st.session_state['history'] = pd.concat([st.session_state['history'], pd.DataFrame([rec])], ignore_index=True)
                save_data()
                time.sleep(1)
                st.rerun()

elif page == "📦 商品建檔與庫存表":
    st.subheader("📦 商品資料庫")
    
    # 若無資料，在主畫面也顯示上傳按鈕，方便使用者
    if st.session_state['inventory'].empty:
        st.info("👋 歡迎！目前沒有資料，您可以手動建立，或直接上傳 Excel。")
        uploaded_file = st.file_uploader("📂 點擊這裡上傳 Excel 商品清單", type=['xlsx', 'xls', 'csv'], key="main_up")
        if uploaded_file and st.button("確認匯入", key="main_btn"):
            df = process_excel_upload(uploaded_file)
            if df is not None:
                st.session_state['inventory'] = df
                save_data()
                st.success("匯入成功！")
                time.sleep(1)
                st.rerun()
        st.divider()

    tab1, tab2 = st.tabs(["✨ 手動建檔", "📋 庫存清單"])
    with tab1:
        with st.form("new_item"):
            c1, c2 = st.columns(2)
            cat = c1.selectbox("分類", get_options(st.session_state['inventory'], '分類', DEFAULT_CATEGORIES))
            ser = c2.selectbox("系列", get_options(st.session_state['inventory'], '系列', DEFAULT_SERIES))
            name = st.text_input("品名")
            sku = st.text_input("貨號", value=generate_sku(cat, st.session_state['inventory']))
            if st.form_submit_button("建立"):
                new_row = {'貨號': sku, '系列': ser, '分類': cat, '品名': name, '庫存數量': 0, '平均成本': 0}
                st.session_state['inventory'] = pd.concat([st.session_state['inventory'], pd.DataFrame([new_row])], ignore_index=True)
                save_data()
                st.success(f"已建立 {name}")
                st.rerun()
                
    with tab2:
        st.dataframe(st.session_state['inventory'], use_container_width=True)

elif page == "📜 歷史紀錄":
    st.subheader("📜 歷史紀錄 (Excel總表)")
    st.data_editor(st.session_state['history'], use_container_width=True, num_rows="dynamic", key="hist_edit")
    if st.button("💾 儲存修改"):
        # st.session_state['history'] = ... (data_editor自動更新state, 這裡只需存檔)
        save_data()
        st.success("已更新")
