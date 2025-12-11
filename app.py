import streamlit as st
import pandas as pd
from datetime import date, datetime
import os
import time
import io

# ==========================================
# 1. 系統設定
# ==========================================

PAGE_TITLE = "全方位製造庫存管理系統"
INVENTORY_FILE = 'inventory_mfg_v1.csv'
HISTORY_FILE = 'history_mfg_v1.csv'

# 定義倉庫 (可依需求修改)
WAREHOUSES = ["原物料倉", "半成品倉", "成品倉", "報廢倉"]

# --- 核心流水帳 (新增 '單據類型' 來區分用途) ---
# 單據類型: 進貨 / 銷售出貨 / 製造領料 / 製造入庫 / 調整
HISTORY_COLUMNS = [
    '單據類型', # <--- 核心欄位：用來區分這筆是進貨、出貨還是製造
    '單號', '日期', '系列', '分類', '品名', '貨號', '批號',
    '倉庫', '數量', 'Key單者',
    '訂單單號', '出貨日期', '貨號備註', '運費', 
    '款項結清', '工資', '發票', '備註'
]

# --- 庫存狀態表 ---
INVENTORY_COLUMNS = [
    '貨號', '系列', '分類', '品名', 
    '總庫存', '均價',
    '庫存_原物料倉', '庫存_半成品倉', '庫存_成品倉', '庫存_報廢倉'
]

DEFAULT_SERIES = ["原料", "半成品", "成品", "包材"]
DEFAULT_CATEGORIES = ["天然石", "金屬配件", "線材", "包裝盒", "完成品"]
DEFAULT_KEYERS = ["Wen", "廠長", "倉管", "業務"]

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
            hist_df['數量'] = pd.to_numeric(hist_df['數量'], errors='coerce').fillna(0)
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

def recalculate_inventory(hist_df, current_inv_df):
    """
    重算庫存核心邏輯：
    - 進貨 / 製造入庫 -> 加庫存
    - 銷售出貨 / 製造領料 -> 減庫存
    """
    new_inv = current_inv_df.copy()
    
    # 重置數量
    cols_reset = ['總庫存'] + [f'庫存_{w}' for w in WAREHOUSES]
    for col in cols_reset:
        new_inv[col] = 0.0
    
    for idx, row in new_inv.iterrows():
        sku = str(row['貨號'])
        target_hist = hist_df[hist_df['貨號'].astype(str) == sku]
        
        total = 0
        w_stock = {w: 0 for w in WAREHOUSES}
        
        for _, h_row in target_hist.iterrows():
            qty = float(h_row['數量'])
            w_name = str(h_row['倉庫'])
            if w_name not in WAREHOUSES: w_name = WAREHOUSES[0]
            
            # 判斷加減邏輯
            # 加項：進貨、製造入庫
            if h_row['單據類型'] in ['進貨', '製造入庫']:
                total += qty
                if w_name in w_stock: w_stock[w_name] += qty
            
            # 減項：銷售出貨、製造領料
            elif h_row['單據類型'] in ['銷售出貨', '製造領料']:
                total -= qty
                if w_name in w_stock: w_stock[w_name] -= qty
        
        new_inv.at[idx, '總庫存'] = total
        for w in WAREHOUSES:
            new_inv.at[idx, f'庫存_{w}'] = w_stock[w]
            
    return new_inv

def gen_batch_number(prefix="BAT"):
    return f"{prefix}-{datetime.now().strftime('%y%m%d%H%M')}"

def convert_to_excel_all_sheets(inv_df, hist_df):
    """產生包含四個分頁的 Excel"""
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        # 1. 總表
        inv_df.to_excel(writer, index=False, sheet_name='庫存總表')
        
        # 2. 進貨紀錄
        df_in = hist_df[hist_df['單據類型'] == '進貨']
        df_in.to_excel(writer, index=False, sheet_name='進貨紀錄')
        
        # 3. 製造紀錄 (含領料與入庫)
        df_mfg = hist_df[hist_df['單據類型'].str.contains('製造')]
        df_mfg.to_excel(writer, index=False, sheet_name='製造紀錄')
        
        # 4. 出貨紀錄
        df_out = hist_df[hist_df['單據類型'].isin(['銷售出貨', '製造領料'])]
        df_out.to_excel(writer, index=False, sheet_name='出貨紀錄')
        
        # 5. 完整流水帳 (Backup)
        hist_df.to_excel(writer, index=False, sheet_name='完整流水帳')
        
    return output.getvalue()

# ==========================================
# 3. 初始化
# ==========================================

if 'inventory' not in st.session_state:
    inv, hist = load_data()
    st.session_state['inventory'] = inv
    st.session_state['history'] = hist

# ==========================================
# 4. 介面邏輯
# ==========================================

st.set_page_config(page_title=PAGE_TITLE, layout="wide", page_icon="🏭")
st.title(f"🏭 {PAGE_TITLE}")

# --- 側邊欄導航 ---
with st.sidebar:
    st.header("部門功能導航")
    
    # 依照您的需求拆分四大區塊
    page = st.radio("選擇作業", [
        "📥 進貨庫存表 (採購)", 
        "🔨 商品製造表 (工廠)", 
        "🚚 商品出貨表 (出貨)", 
        "📊 總表監控 (管理)",
        "📦 商品建檔與維護"
    ])
    
    st.divider()
    st.header("💾 報表中心")
    if not st.session_state['history'].empty:
        st.caption("下載包含所有分頁的完整 Excel")
        excel_data = convert_to_excel_all_sheets(st.session_state['inventory'], st.session_state['history'])
        st.download_button(
            label="📥 下載完整四合一報表",
            data=excel_data,
            file_name=f'Factory_Report_{date.today()}.xlsx',
            mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )

# ---------------------------------------------------------
# 頁面 1: 進貨庫存表 (只看進貨)
# ---------------------------------------------------------
if page == "📥 進貨庫存表 (採購)":
    st.subheader("📥 進貨紀錄表")
    st.info("此區僅顯示「外部進貨」紀錄。")
    
    # 1. 新增進貨單
    with st.expander("➕ 新增進貨單 (Purchase)", expanded=False):
        inv_df = st.session_state['inventory']
        if inv_df.empty:
            st.warning("請先建立商品資料")
        else:
            inv_df['label'] = inv_df['貨號'] + " | " + inv_df['品名']
            c1, c2, c3 = st.columns([2, 1, 1])
            p_sel = c1.selectbox("選擇進貨商品", inv_df['label'].tolist(), key="in_sel")
            p_wh = c2.selectbox("入庫倉庫", WAREHOUSES, index=0, key="in_wh") # 預設原物料倉
            p_qty = c3.number_input("進貨數量", 1, key="in_qty")
            
            c4, c5, c6 = st.columns(3)
            p_date = c4.date_input("進貨日期", date.today(), key="in_date")
            p_batch = c5.text_input("批號 (自動產生)", value=gen_batch_number("IN"), key="in_batch")
            p_user = c6.selectbox("Key單者", DEFAULT_KEYERS, key="in_user")
            p_note = st.text_input("備註 (廠商/採購單號)", key="in_note")
            
            if st.button("確認進貨", type="primary"):
                p_row = inv_df[inv_df['label'] == p_sel].iloc[0]
                rec = {
                    '單據類型': '進貨', # 固定
                    '單號': datetime.now().strftime('%Y%m%d%H%M%S'),
                    '日期': p_date, '系列': p_row['系列'], '分類': p_row['分類'], 
                    '品名': p_row['品名'], '貨號': p_row['貨號'], '批號': p_batch,
                    '倉庫': p_wh, '數量': p_qty, 'Key單者': p_user, '備註': p_note
                }
                st.session_state['history'] = pd.concat([st.session_state['history'], pd.DataFrame([rec])], ignore_index=True)
                # 自動重算
                st.session_state['inventory'] = recalculate_inventory(st.session_state['history'], st.session_state['inventory'])
                save_data()
                st.success("進貨單已建立！")
                time.sleep(1)
                st.rerun()

    # 2. 查看表格 (Filter: 進貨)
    df = st.session_state['history']
    if not df.empty:
        # 只篩選 "進貨"
        df_view = df[df['單據類型'] == '進貨'].copy()
        st.dataframe(df_view, use_container_width=True)

# ---------------------------------------------------------
# 頁面 2: 商品製造表 (領料 + 入庫)
# ---------------------------------------------------------
elif page == "🔨 商品製造表 (工廠)":
    st.subheader("🔨 製造生產紀錄")
    st.info("此區管理「原料消耗 (領料)」與「成品產出 (入庫)」。")
    
    tab1, tab2, tab3 = st.tabs(["📤 領料 (扣庫存)", "📥 完工入庫 (加庫存)", "📋 製造紀錄明細"])
    
    inv_df = st.session_state['inventory']
    inv_df['label'] = inv_df['貨號'] + " | " + inv_df['品名'] + " | 總存:" + inv_df['總庫存'].astype(str)

    # --- 領料 ---
    with tab1:
        st.caption("從倉庫領取原料，庫存將會扣除。")
        with st.form("mfg_out_form"):
            c1, c2 = st.columns([2, 1])
            m_sel = c1.selectbox("選擇原料", inv_df['label'].tolist())
            m_wh = c2.selectbox("領料倉庫", WAREHOUSES, index=0) # 預設原物料
            
            c3, c4, c5 = st.columns(3)
            m_qty = c3.number_input("領用數量", 1)
            m_date = c4.date_input("領料日期", date.today())
            m_user = c5.selectbox("領料人", DEFAULT_KEYERS)
            m_mo = st.text_input("工單單號 (MO Number)")
            
            if st.form_submit_button("❌ 確認領料 (扣帳)"):
                m_row = inv_df[inv_df['label'] == m_sel].iloc[0]
                rec = {
                    '單據類型': '製造領料', # 標記為製造用途
                    '單號': datetime.now().strftime('%Y%m%d%H%M%S'),
                    '日期': m_date, '系列': m_row['系列'], '分類': m_row['分類'], 
                    '品名': m_row['品名'], '貨號': m_row['貨號'], '批號': '',
                    '倉庫': m_wh, '數量': m_qty, 'Key單者': m_user, 
                    '訂單單號': m_mo, '備註': f"工單:{m_mo} 領料"
                }
                st.session_state['history'] = pd.concat([st.session_state['history'], pd.DataFrame([rec])], ignore_index=True)
                st.session_state['inventory'] = recalculate_inventory(st.session_state['history'], st.session_state['inventory'])
                save_data()
                st.success(f"已領料 {m_qty} 個")
                time.sleep(1)
                st.rerun()

    # --- 完工 ---
    with tab2:
        st.caption("生產完成，成品進入倉庫。")
        with st.form("mfg_in_form"):
            c1, c2 = st.columns([2, 1])
            f_sel = c1.selectbox("選擇成品", inv_df['label'].tolist())
            f_wh = c2.selectbox("入庫倉庫", WAREHOUSES, index=2) # 預設成品倉
            
            c3, c4, c5 = st.columns(3)
            f_qty = c3.number_input("產出數量", 1)
            f_date = c4.date_input("完工日期", date.today())
            f_batch = c5.text_input("成品批號", value=gen_batch_number("PD"))
            f_user = st.selectbox("Key單者", DEFAULT_KEYERS)
            f_mo = st.text_input("關聯工單 (MO Number)")
            
            if st.form_submit_button("✅ 確認完工入庫"):
                f_row = inv_df[inv_df['label'] == f_sel].iloc[0]
                rec = {
                    '單據類型': '製造入庫',
                    '單號': datetime.now().strftime('%Y%m%d%H%M%S'),
                    '日期': f_date, '系列': f_row['系列'], '分類': f_row['分類'], 
                    '品名': f_row['品名'], '貨號': f_row['貨號'], '批號': f_batch,
                    '倉庫': f_wh, '數量': f_qty, 'Key單者': f_user, 
                    '訂單單號': f_mo, '備註': f"工單:{f_mo} 完工"
                }
                st.session_state['history'] = pd.concat([st.session_state['history'], pd.DataFrame([rec])], ignore_index=True)
                st.session_state['inventory'] = recalculate_inventory(st.session_state['history'], st.session_state['inventory'])
                save_data()
                st.success(f"成品已入庫 {f_qty} 個")
                time.sleep(1)
                st.rerun()

    # --- 紀錄 ---
    with tab3:
        df = st.session_state['history']
        if not df.empty:
            # 篩選 "製造" 相關
            mask = df['單據類型'].astype(str).str.contains('製造')
            st.dataframe(df[mask], use_container_width=True)

# ---------------------------------------------------------
# 頁面 3: 商品出貨表 (出貨)
# ---------------------------------------------------------
elif page == "🚚 商品出貨表 (出貨)":
    st.subheader("🚚 出貨紀錄表")
    st.info("此區顯示「銷售出貨」以及「製造領料」的所有出庫紀錄。")
    
    with st.expander("➖ 新增銷售出貨單", expanded=False):
        inv_df = st.session_state['inventory']
        inv_df['label'] = inv_df['貨號'] + " | " + inv_df['品名'] + " | 庫存:" + inv_df['庫存_成品倉'].astype(str)
        
        with st.form("sales_form"):
            c1, c2 = st.columns([2, 1])
            s_sel = c1.selectbox("選擇商品", inv_df['label'].tolist())
            s_wh = c2.selectbox("出貨倉庫", WAREHOUSES, index=2) # 預設成品倉
            
            c3, c4 = st.columns(2)
            s_qty = c3.number_input("出貨數量", 1)
            s_date = c4.date_input("出貨日期", date.today())
            
            s_order = st.text_input("客戶訂單號")
            s_user = st.selectbox("Key單者", DEFAULT_KEYERS)
            s_note = st.text_area("備註")
            
            if st.form_submit_button("確認出貨 (扣帳)"):
                s_row = inv_df[inv_df['label'] == s_sel].iloc[0]
                rec = {
                    '單據類型': '銷售出貨',
                    '單號': datetime.now().strftime('%Y%m%d%H%M%S'),
                    '日期': s_date, '系列': s_row['系列'], '分類': s_row['分類'], 
                    '品名': s_row['品名'], '貨號': s_row['貨號'], '批號': '',
                    '倉庫': s_wh, '數量': s_qty, 'Key單者': s_user, 
                    '訂單單號': s_order, '備註': s_note
                }
                st.session_state['history'] = pd.concat([st.session_state['history'], pd.DataFrame([rec])], ignore_index=True)
                st.session_state['inventory'] = recalculate_inventory(st.session_state['history'], st.session_state['inventory'])
                save_data()
                st.success("出貨成功！")
                time.sleep(1)
                st.rerun()

    # 表格顯示 (Filter: 銷售出貨 + 製造領料)
    df = st.session_state['history']
    if not df.empty:
        # 顯示所有 "減少庫存" 的動作
        mask = df['單據類型'].isin(['銷售出貨', '製造領料'])
        st.dataframe(df[mask], use_container_width=True)

# ---------------------------------------------------------
# 頁面 4: 總表監控
# ---------------------------------------------------------
elif page == "📊 總表監控 (管理)":
    st.subheader("📊 庫存與流水帳總表")
    
    tab_inv, tab_hist = st.tabs(["📦 即時庫存總表", "📜 完整流水帳"])
    
    with tab_inv:
        st.caption("各倉庫即時庫存狀況")
        df_inv = st.session_state['inventory']
        if not df_inv.empty:
            st.dataframe(
                df_inv, 
                use_container_width=True,
                column_config={
                    "總庫存": st.column_config.NumberColumn(format="%d", help="所有倉庫加總"),
                    "庫存_原物料倉": st.column_config.NumberColumn(format="%d"),
                    "庫存_半成品倉": st.column_config.NumberColumn(format="%d"),
                    "庫存_成品倉": st.column_config.NumberColumn(format="%d"),
                }
            )
    
    with tab_hist:
        st.caption("所有進出紀錄 (含進貨、出貨、製造)")
        df_hist = st.session_state['history']
        if not df_hist.empty:
            # 搜尋
            search = st.text_input("🔍 全局搜尋 (單號/品名/工單)", "")
            if search:
                mask = df_hist.astype(str).apply(lambda x: x.str.contains(search, case=False)).any(axis=1)
                df_hist = df_hist[mask]
            
            # 編輯器
            edited_df = st.data_editor(df_hist, use_container_width=True, num_rows="dynamic", height=600)
            if st.button("💾 儲存修正"):
                st.session_state['history'] = edited_df
                st.session_state['inventory'] = recalculate_inventory(edited_df, st.session_state['inventory'])
                save_data()
                st.success("總表已修正")

# ---------------------------------------------------------
# 頁面 5: 商品建檔
# ---------------------------------------------------------
elif page == "📦 商品建檔與維護":
    st.subheader("📦 商品資料庫")
    with st.form("new_prod"):
        c1, c2 = st.columns(2)
        cat = c1.selectbox("分類", DEFAULT_CATEGORIES)
        ser = c2.selectbox("系列", DEFAULT_SERIES)
        name = st.text_input("品名")
        sku = st.text_input("貨號 (唯一識別)", value=f"P-{int(time.time())}")
        
        if st.form_submit_button("建立新商品"):
            new_row = {'貨號': sku, '系列': ser, '分類': cat, '品名': name, '總庫存': 0, '均價': 0}
            # 初始化各倉
            for w in WAREHOUSES: new_row[f'庫存_{w}'] = 0
                
            st.session_state['inventory'] = pd.concat([st.session_state['inventory'], pd.DataFrame([new_row])], ignore_index=True)
            save_data()
            st.success(f"已建立：{name}")
            time.sleep(1)
            st.rerun()
            
    st.divider()
    st.dataframe(st.session_state['inventory'])
