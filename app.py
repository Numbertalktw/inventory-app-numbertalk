import streamlit as st
import pandas as pd
from datetime import date, datetime
import os
import time

# ==========================================
# 1. 系統設定
# ==========================================

PAGE_TITLE = "商品庫存 & 出貨管理系統"
INVENTORY_FILE = 'inventory_data_final.csv'
HISTORY_FILE = 'history_data_final.csv'

# --- 商品出貨表 (流水帳) 18 欄位 ---
# 對應您的 Excel 截圖 A~R 欄
HISTORY_COLUMNS = [
    '單號', '日期', '系列', '分類', '品名', '貨號', 
    '出庫單號(可複寫)', '出入庫', '數量', '經手人', 
    '訂單單號', '出貨日期', '貨號備註', '運費', 
    '款項結清', '工資', '發票', '備註'
]

# --- 商品庫存表 (總量狀態) ---
# 用於快速查看還有多少貨
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
            inv_df['貨號'] = inv_df['貨號'].astype(str)
        except:
            inv_df = pd.DataFrame(columns=INVENTORY_COLUMNS)
    else:
        inv_df = pd.DataFrame(columns=INVENTORY_COLUMNS)

    # 紀錄
    if os.path.exists(HISTORY_FILE):
        try:
            hist_df = pd.read_csv(HISTORY_FILE)
            # 確保欄位順序正確
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

def process_history_upload(file_obj):
    """處理 18 欄位的出貨表上傳"""
    try:
        if file_obj.name.endswith('.csv'):
            df = pd.read_csv(file_obj)
        else:
            df = pd.read_excel(file_obj)
        
        # 簡單的欄位名稱容錯 (若 Excel 標題有些微差異)
        rename_map = {
            '出庫單號': '出庫單號(可複寫)',
            '商品貨號': '貨號', '商品品名': '品名', '商品系列': '系列', '商品分類': '分類'
        }
        df = df.rename(columns=rename_map)

        # 確保擁有所有 18 個欄位，沒有的補空值
        for col in HISTORY_COLUMNS:
            if col not in df.columns:
                df[col] = ""
        
        # 格式整理
        df['單號'] = df['單號'].astype(str)
        df['貨號'] = df['貨號'].astype(str)
        
        return df[HISTORY_COLUMNS]
    except Exception as e:
        st.error(f"出貨表解析失敗: {e}")
        return None

def process_inventory_upload(file_obj):
    """處理庫存表上傳"""
    try:
        if file_obj.name.endswith('.csv'):
            df = pd.read_csv(file_obj)
        else:
            df = pd.read_excel(file_obj)
        
        col_map = {
            '名稱': '品名', '商品名稱': '品名',
            '數量': '庫存數量', '庫存': '庫存數量',
            '成本': '平均成本', '單價': '平均成本',
            '類別': '分類', '商品分類': '分類'
        }
        df = df.rename(columns=col_map)
        
        for col in INVENTORY_COLUMNS:
            if col not in df.columns:
                if col == '庫存數量' or col == '平均成本': df[col] = 0
                else: df[col] = ""
        
        df['貨號'] = df['貨號'].astype(str).replace('nan', '')
        # 若無貨號自動產生
        for idx, row in df.iterrows():
            if not row['貨號'] or row['貨號'] == 'nan':
                 df.at[idx, '貨號'] = f"AUTO-{idx}"

        return df[INVENTORY_COLUMNS]
    except Exception as e:
        st.error(f"庫存表解析失敗: {e}")
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

# --- 側邊欄：清楚區分兩個表的管理 ---
with st.sidebar:
    st.header("功能導航")
    page = st.radio("前往", ["📊 商品庫存表 (總量)", "🚚 商品出貨表 (流水帳)", "➕ 新增出入庫單據"])
    
    st.divider()
    st.header("💾 資料匯入/匯出")
    
    # 區塊 1: 出貨表管理
    with st.expander("🚚 出貨表 (Excel 18欄)", expanded=True):
        st.caption("上傳完整的進出貨紀錄")
        up_hist = st.file_uploader("上傳出貨紀錄 (.xlsx)", type=['xlsx', 'xls', 'csv'], key="hist_up")
        if up_hist and st.button("確認匯入出貨表"):
            new_hist = process_history_upload(up_hist)
            if new_hist is not None:
                st.session_state['history'] = new_hist
                
                # 自動建立庫存清單 (方便使用者)
                if not new_hist.empty and st.session_state['inventory'].empty:
                    unique_items = new_hist[['貨號', '系列', '分類', '品名']].drop_duplicates(subset=['貨號'])
                    unique_items['庫存數量'] = 0 
                    unique_items['平均成本'] = 0
                    st.session_state['inventory'] = unique_items[INVENTORY_COLUMNS]
                    st.success(f"已匯入紀錄，並自動建立 {len(unique_items)} 筆商品資料！")
                
                save_data()
                st.rerun()
                
        if not st.session_state['history'].empty:
            csv_h = st.session_state['history'].to_csv(index=False).encode('utf-8-sig')
            st.download_button("📥 下載出貨表", csv_h, f'Shipment_History_{date.today()}.csv', "text/csv")

    # 區塊 2: 庫存表管理
    with st.expander("📊 庫存表 (庫存盤點)", expanded=False):
        st.caption("上傳當下的庫存數量")
        up_inv = st.file_uploader("上傳庫存清單 (.xlsx)", type=['xlsx', 'xls', 'csv'], key="inv_up")
        if up_inv and st.button("確認匯入庫存表"):
            new_inv = process_inventory_upload(up_inv)
            if new_inv is not None:
                st.session_state['inventory'] = new_inv
                save_data()
                st.rerun()
        
        if not st.session_state['inventory'].empty:
            csv_i = st.session_state['inventory'].to_csv(index=False).encode('utf-8-sig')
            st.download_button("📥 下載庫存表", csv_i, f'Inventory_Stock_{date.today()}.csv', "text/csv")

# ---------------------------------------------------------
# 頁面 1: 商品庫存表
# ---------------------------------------------------------
if page == "📊 商品庫存表 (總量)":
    st.subheader("📊 商品庫存表")
    st.caption("此表顯示目前倉庫內的「剩餘數量」與「成本狀態」。")
    
    df_inv = st.session_state['inventory']
    if df_inv.empty:
        st.info("目前無庫存資料。請從左側上傳 Excel，或前往「新增出入庫單據」建立。")
    else:
        # 簡單的統計指標
        total_items = len(df_inv)
        total_stock = df_inv['庫存數量'].sum()
        c1, c2 = st.columns(2)
        c1.metric("商品品項數", f"{total_items} 款")
        c2.metric("庫存總數量", f"{total_stock:,.0f} 個")
        
        st.dataframe(
            df_inv, 
            use_container_width=True,
            column_config={
                "庫存數量": st.column_config.NumberColumn(format="%d"),
                "平均成本": st.column_config.NumberColumn(format="$%.2f")
            }
        )

# ---------------------------------------------------------
# 頁面 2: 商品出貨表 (您的 Excel 18 欄位)
# ---------------------------------------------------------
elif page == "🚚 商品出貨表 (流水帳)":
    st.subheader("🚚 商品出貨表 (歷史紀錄)")
    st.caption("此表顯示完整的 18 欄位進出貨明細 (對應您的 Excel)。")
    
    df_hist = st.session_state['history']
    if df_hist.empty:
        st.warning("目前無出貨紀錄。請從左側「出貨表」區塊上傳您的 Excel。")
    else:
        # 搜尋功能
        search = st.text_input("🔍 搜尋 (單號/品名/訂單)", "")
        if search:
            mask = df_hist.astype(str).apply(lambda x: x.str.contains(search, case=False)).any(axis=1)
            df_hist = df_hist[mask]
            
        # 可編輯的表格
        edited_df = st.data_editor(
            df_hist,
            use_container_width=True,
            num_rows="dynamic",
            height=600,
            key="hist_editor"
        )
        
        if st.button("💾 儲存表格修改"):
            st.session_state['history'] = edited_df
            save_data()
            st.success("已更新出貨表！")

# ---------------------------------------------------------
# 頁面 3: 新增單據
# ---------------------------------------------------------
elif page == "➕ 新增出入庫單據":
    st.subheader("➕ 新增出入庫單據")
    st.caption("在此輸入每一筆異動，系統會同時寫入「出貨表」並更新「庫存表」數量。")
    
    inv_df = st.session_state['inventory']
    
    if inv_df.empty:
        st.warning("⚠️ 請先建立商品資料 (可透過左側上傳出貨表 Excel 自動建立)。")
    else:
        # 製作選單標籤
        inv_df['label'] = inv_df['貨號'] + " | " + inv_df['品名'] + " | 剩餘:" + inv_df['庫存數量'].astype(str)
        
        c_sel, c_act = st.columns([2, 1])
        with c_sel:
            selected_label = st.selectbox("選擇商品", inv_df['label'].tolist())
            row = inv_df[inv_df['label'] == selected_label].iloc[0]
            idx = inv_df[inv_df['label'] == selected_label].index[0]
        with c_act:
            act = st.radio("動作", ["入庫", "出庫"], horizontal=True)

        st.divider()

        with st.form("transaction_entry"):
            # 1. 基本 (自動產生 A, 手填 B, I, J)
            col1, col2, col3 = st.columns(3)
            txn_date = col1.date_input("日期", value=date.today())
            qty = col2.number_input("數量", min_value=1, value=1)
            handler = col3.selectbox("經手人", DEFAULT_HANDLERS)
            
            st.info(f"商品資訊：{row['系列']} - {row['分類']} - {row['品名']} ({row['貨號']})")
            
            # 2. 單據 (G, K, L, M)
            col4, col5, col6, col7 = st.columns(4)
            order_id = col4.text_input("訂單單號")
            ship_date = col5.date_input("出貨日期", value=date.today())
            out_id = col6.text_input("出庫單號 (可複寫)")
            sku_note = col7.text_input("貨號備註")

            # 3. 費用 (N, O, P, Q)
            col8, col9, col10, col11 = st.columns(4)
            fee = col8.text_input("運費")
            pay = col9.selectbox("款項結清", ["", "是", "否", "部分"])
            labor = col10.text_input("工資")
            inv_no = col11.text_input("發票")
            
            note = st.text_area("備註")
            
            # 成本輸入 (僅入庫用)
            cost_in = 0
            if act == "入庫":
                cost_in = st.number_input("本次進貨總成本 (更新平均成本用)", min_value=0)

            if st.form_submit_button("✅ 確認新增"):
                # 1. 準備資料
                now_str = datetime.now().strftime('%Y%m%d%H%M%S')
                record_id = f"{now_str}"
                
                final_out_id = out_id
                if act == "出庫" and not final_out_id:
                    final_out_id = f"OUT-{datetime.now().strftime('%Y%m%d')}"
                
                io_str = f"{act}-{handler}"

                # 2. 更新庫存表 (Stock)
                curr_qty = float(row['庫存數量'])
                curr_cost = float(row['平均成本'])
                
                if act == "入庫":
                    new_qty = curr_qty + qty
                    new_avg = ((curr_qty * curr_cost) + cost_in) / new_qty if new_qty > 0 else 0
                    st.session_state['inventory'].at[idx, '庫存數量'] = new_qty
                    st.session_state['inventory'].at[idx, '平均成本'] = new_avg
                else:
                    new_qty = curr_qty - qty
                    st.session_state['inventory'].at[idx, '庫存數量'] = new_qty
                
                # 3. 寫入出貨表 (History - 18 Cols)
                new_rec = {
                    '單號': record_id, '日期': txn_date,
                    '系列': row['系列'], '分類': row['分類'], '品名': row['品名'], '貨號': row['貨號'],
                    '出庫單號(可複寫)': final_out_id, '出入庫': io_str,
                    '數量': qty, '經手人': handler,
                    '訂單單號': order_id, '出貨日期': ship_date if act == '出庫' else None,
                    '貨號備註': sku_note, '運費': fee,
                    '款項結清': pay, '工資': labor, '發票': inv_no, '備註': note
                }
                
                st.session_state['history'] = pd.concat([st.session_state['history'], pd.DataFrame([new_rec])], ignore_index=True)
                save_data()
                st.success("已成功新增單據！")
                time.sleep(1)
                st.rerun()
