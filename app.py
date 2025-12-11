import streamlit as st
import pandas as pd
from datetime import date, datetime
import os
import time
import io

# ==========================================
# 1. 系統設定
# ==========================================

PAGE_TITLE = "製造庫存系統"
INVENTORY_FILE = 'inventory_secure_v2.csv'
HISTORY_FILE = 'history_secure_v2.csv'
ADMIN_PASSWORD = "8888"  # 管理員密碼

# 定義倉庫
WAREHOUSES = ["原物料倉", "半成品倉", "成品倉", "報廢倉"]

# --- 核心流水帳 ---
HISTORY_COLUMNS = [
    '單據類型', '單號', '日期', '系列', '分類', '品名', '貨號', '批號',
    '倉庫', '數量', 'Key單者',
    '訂單單號', '出貨日期', '貨號備註', '運費', 
    '款項結清', '工資', '發票', '備註',
    '進貨總成本' 
]

# --- 庫存狀態表 ---
INVENTORY_COLUMNS = [
    '貨號', '系列', '分類', '品名', 
    '總庫存', '均價', 
    '庫存_原物料倉', '庫存_半成品倉', '庫存_成品倉', '庫存_報廢倉'
]

DEFAULT_SERIES = ["原料", "半成品", "成品", "包材"]
DEFAULT_CATEGORIES = ["天然石", "金屬配件", "線材", "包裝盒", "完成品"]
DEFAULT_KEYERS = ["Wen", "廠長", "倉管", "業務", "出貨人員"]

# ==========================================
# 2. 核心函式
# ==========================================

def load_data():
    """讀取 CSV 資料"""
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
                    hist_df[col] = "" if col not in ['數量', '進貨總成本', '運費', '工資'] else 0
            hist_df = hist_df[HISTORY_COLUMNS]
            for c in ['數量', '進貨總成本', '運費', '工資']:
                hist_df[c] = pd.to_numeric(hist_df[c], errors='coerce').fillna(0)
        except:
            hist_df = pd.DataFrame(columns=HISTORY_COLUMNS)
    else:
        hist_df = pd.DataFrame(columns=HISTORY_COLUMNS)
        
    return inv_df, hist_df

def save_data():
    """儲存 CSV 資料"""
    if 'inventory' in st.session_state:
        st.session_state['inventory'].to_csv(INVENTORY_FILE, index=False, encoding='utf-8-sig')
    if 'history' in st.session_state:
        st.session_state['history'].to_csv(HISTORY_FILE, index=False, encoding='utf-8-sig')

def recalculate_inventory(hist_df, current_inv_df):
    """
    [核心邏輯] 根據流水帳重新計算庫存與均價
    """
    # 1. 準備商品清單 (從歷史紀錄中提取所有出現過的商品)
    # 這樣即使上傳新的 Excel，也能自動建立新商品
    if not hist_df.empty:
        unique_items = hist_df[['貨號', '系列', '分類', '品名']].drop_duplicates(subset=['貨號']).copy()
        # 補齊庫存欄位
        for col in INVENTORY_COLUMNS:
            if col not in unique_items.columns:
                unique_items[col] = 0.0
        new_inv = unique_items[INVENTORY_COLUMNS].reset_index(drop=True)
    else:
        new_inv = current_inv_df.copy()
        # 重置數量
        cols_reset = ['總庫存', '均價'] + [f'庫存_{w}' for w in WAREHOUSES]
        for col in cols_reset:
            new_inv[col] = 0.0
    
    # 2. 開始計算
    for idx, row in new_inv.iterrows():
        sku = str(row['貨號'])
        target_hist = hist_df[hist_df['貨號'].astype(str) == sku]
        
        total_qty = 0
        total_value = 0.0
        w_stock = {w: 0 for w in WAREHOUSES}
        
        for _, h_row in target_hist.iterrows():
            qty = float(h_row['數量'])
            cost_total = float(h_row['進貨總成本'])
            doc_type = str(h_row['單據類型'])
            w_name = str(h_row['倉庫'])
            if w_name not in WAREHOUSES: w_name = WAREHOUSES[0]
            
            # 加項 (進貨/製造入庫)
            if doc_type in ['進貨', '製造入庫', '調整入庫']:
                if cost_total > 0:
                    total_value += cost_total
                total_qty += qty
                if w_name in w_stock: w_stock[w_name] += qty
            
            # 減項 (出貨/領料)
            elif doc_type in ['銷售出貨', '製造領料', '調整出庫']:
                current_avg = (total_value / total_qty) if total_qty > 0 else 0
                total_qty -= qty
                total_value -= (qty * current_avg)
                if w_name in w_stock: w_stock[w_name] -= qty

        new_inv.at[idx, '總庫存'] = total_qty
        new_inv.at[idx, '均價'] = (total_value / total_qty) if total_qty > 0 else 0
        for w in WAREHOUSES:
            new_inv.at[idx, f'庫存_{w}'] = w_stock[w]
            
    return new_inv

def gen_batch_number(prefix="BAT"):
    return f"{prefix}-{datetime.now().strftime('%y%m%d%H%M')}"

def get_safe_view(df):
    sensitive_cols = ['進貨總成本', '均價', '工資', '款項結清']
    safe_cols = [c for c in df.columns if c not in sensitive_cols]
    return df[safe_cols]

def convert_to_excel_all_sheets(inv_df, hist_df):
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        inv_df.to_excel(writer, index=False, sheet_name='庫存總表')
        
        if '單據類型' in hist_df.columns:
            df_in = hist_df[hist_df['單據類型'] == '進貨']
            df_in.to_excel(writer, index=False, sheet_name='進貨紀錄')
            
            df_mfg = hist_df[hist_df['單據類型'].str.contains('製造', na=False)]
            df_mfg.to_excel(writer, index=False, sheet_name='製造紀錄')
            
            df_out = hist_df[hist_df['單據類型'].isin(['銷售出貨', '製造領料'])]
            df_out.to_excel(writer, index=False, sheet_name='出貨紀錄')
        
        hist_df.to_excel(writer, index=False, sheet_name='完整流水帳')
    return output.getvalue()

def process_upload(file_obj):
    """處理上傳的 Excel"""
    try:
        # 1. 讀取檔案
        if file_obj.name.endswith('.csv'):
            df_raw = pd.read_csv(file_obj)
        else:
            # 優先嘗試讀取 '完整流水帳' sheet，若無則讀第一個 sheet
            xls = pd.ExcelFile(file_obj)
            if '完整流水帳' in xls.sheet_names:
                df_raw = pd.read_excel(file_obj, sheet_name='完整流水帳')
            else:
                df_raw = pd.read_excel(file_obj)
        
        # 2. 欄位對應與補齊
        # (這裡可以加入 rename_columns 的邏輯以相容舊格式)
        for col in HISTORY_COLUMNS:
            if col not in df_raw.columns:
                df_raw[col] = "" if col not in ['數量', '進貨總成本'] else 0
        
        # 3. 轉型
        df_raw['貨號'] = df_raw['貨號'].astype(str)
        df_raw['數量'] = pd.to_numeric(df_raw['數量'], errors='coerce').fillna(0)
        df_raw['進貨總成本'] = pd.to_numeric(df_raw['進貨總成本'], errors='coerce').fillna(0)
        
        # 4. 只取標準欄位
        new_hist = df_raw[HISTORY_COLUMNS].copy()
        
        return new_hist
        
    except Exception as e:
        st.error(f"檔案讀取失敗: {e}")
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

st.set_page_config(page_title=PAGE_TITLE, layout="wide", page_icon="🚚")
st.title(f"🏭 {PAGE_TITLE}")

with st.sidebar:
    st.header("部門功能導航")
    page = st.radio("選擇作業", [
        "📥 進貨庫存 (無金額)", 
        "🔨 製造生產 (工廠)", 
        "🚚 銷售出貨 (業務/出貨)", 
        "📦 商品建檔與維護",
        "💰 成本與財務管理 (加密)"
    ])
    
    st.divider()
    st.header("💾 資料管理中心")
    
    # --- 下載功能 ---
    if not st.session_state['history'].empty:
        excel_data = convert_to_excel_all_sheets(st.session_state['inventory'], st.session_state['history'])
        st.download_button(
            label="📥 下載完整總表 (Excel)",
            data=excel_data,
            file_name=f'Report_{date.today()}.xlsx',
            mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            help="包含庫存表、進貨、出貨、製造等所有分頁"
        )
    
    # --- 上傳功能 ---
    with st.expander("📤 上傳報表 (還原/匯入)", expanded=False):
        st.info("上傳 Excel 後，系統將根據「完整流水帳」重新計算庫存。")
        up_file = st.file_uploader("選擇 Excel 檔案", type=['xlsx', 'xls', 'csv'])
        
        if up_file and st.button("確認匯入並重算庫存"):
            new_hist = process_upload(up_file)
            if new_hist is not None:
                st.session_state['history'] = new_hist
                # 關鍵：上傳後立即重算庫存
                st.session_state['inventory'] = recalculate_inventory(new_hist, st.session_state['inventory'])
                save_data()
                st.success(f"匯入成功！共 {len(new_hist)} 筆紀錄，庫存已更新。")
                time.sleep(1)
                st.rerun()

# ---------------------------------------------------------
# 頁面 1: 進貨 (一般員工用)
# ---------------------------------------------------------
if page == "📥 進貨庫存 (無金額)":
    st.subheader("📥 進貨點收")
    st.info("進貨僅需輸入數量，金額由財務補登。")
    
    with st.expander("➕ 新增進貨單", expanded=True):
        inv_df = st.session_state['inventory']
        if inv_df.empty:
            st.warning("請先建立商品")
        else:
            inv_df['label'] = inv_df['貨號'] + " | " + inv_df['品名']
            c1, c2, c3 = st.columns([2, 1, 1])
            p_sel = c1.selectbox("進貨商品", inv_df['label'].tolist())
            p_wh = c2.selectbox("入庫倉庫", WAREHOUSES, index=0)
            p_qty = c3.number_input("進貨數量", 1)
            
            c4, c5 = st.columns(2)
            p_date = c4.date_input("進貨日期", date.today())
            p_user = c5.selectbox("Key單者", DEFAULT_KEYERS)
            p_note = st.text_input("備註")
            
            if st.button("確認進貨"):
                p_row = inv_df[inv_df['label'] == p_sel].iloc[0]
                rec = {
                    '單據類型': '進貨',
                    '單號': datetime.now().strftime('%Y%m%d%H%M%S'),
                    '日期': p_date, '系列': p_row['系列'], '分類': p_row['分類'], 
                    '品名': p_row['品名'], '貨號': p_row['貨號'], '批號': gen_batch_number("IN"),
                    '倉庫': p_wh, '數量': p_qty, 'Key單者': p_user, '備註': p_note,
                    '進貨總成本': 0
                }
                st.session_state['history'] = pd.concat([st.session_state['history'], pd.DataFrame([rec])], ignore_index=True)
                st.session_state['inventory'] = recalculate_inventory(st.session_state['history'], st.session_state['inventory'])
                save_data()
                st.success("進貨單已建立！")
                time.sleep(1)
                st.rerun()
    
    df = st.session_state['history']
    if not df.empty:
        df_view = df[df['單據類型'] == '進貨'].copy()
        st.dataframe(get_safe_view(df_view), use_container_width=True)

# ---------------------------------------------------------
# 頁面 2: 製造
# ---------------------------------------------------------
elif page == "🔨 製造生產 (工廠)":
    st.subheader("🔨 製造生產紀錄")
    tab1, tab2 = st.tabs(["📤 領料", "📥 完工"])
    
    inv_df = st.session_state['inventory']
    inv_df['label'] = inv_df['貨號'] + " | " + inv_df['品名'] + " | 總存:" + inv_df['總庫存'].astype(str)

    with tab1:
        with st.form("mfg_out"):
            c1, c2 = st.columns([2, 1])
            m_sel = c1.selectbox("原料", inv_df['label'].tolist())
            m_wh = c2.selectbox("從哪領", WAREHOUSES, index=0)
            m_qty = st.number_input("領用量", 1)
            m_user = st.selectbox("領料人", DEFAULT_KEYERS)
            m_mo = st.text_input("工單單號")
            
            if st.form_submit_button("確認領料"):
                m_row = inv_df[inv_df['label'] == m_sel].iloc[0]
                rec = {
                    '單據類型': '製造領料',
                    '單號': datetime.now().strftime('%Y%m%d%H%M%S'),
                    '日期': date.today(), '系列': m_row['系列'], '分類': m_row['分類'], 
                    '品名': m_row['品名'], '貨號': m_row['貨號'], '批號': '',
                    '倉庫': m_wh, '數量': m_qty, 'Key單者': m_user, '訂單單號': m_mo
                }
                st.session_state['history'] = pd.concat([st.session_state['history'], pd.DataFrame([rec])], ignore_index=True)
                st.session_state['inventory'] = recalculate_inventory(st.session_state['history'], st.session_state['inventory'])
                save_data()
                st.success(f"已領料 {m_qty}")
                time.sleep(1)
                st.rerun()

    with tab2:
        with st.form("mfg_in"):
            c1, c2 = st.columns([2, 1])
            f_sel = c1.selectbox("成品", inv_df['label'].tolist())
            f_wh = c2.selectbox("入庫至", WAREHOUSES, index=2)
            f_qty = st.number_input("產出量", 1)
            f_batch = st.text_input("成品批號", value=gen_batch_number("PD"))
            f_mo = st.text_input("工單單號")
            
            if st.form_submit_button("完工入庫"):
                f_row = inv_df[inv_df['label'] == f_sel].iloc[0]
                rec = {
                    '單據類型': '製造入庫',
                    '單號': datetime.now().strftime('%Y%m%d%H%M%S'),
                    '日期': date.today(), '系列': f_row['系列'], '分類': f_row['分類'], 
                    '品名': f_row['品名'], '貨號': f_row['貨號'], '批號': f_batch,
                    '倉庫': f_wh, '數量': f_qty, 'Key單者': '廠長', '訂單單號': f_mo
                }
                st.session_state['history'] = pd.concat([st.session_state['history'], pd.DataFrame([rec])], ignore_index=True)
                st.session_state['inventory'] = recalculate_inventory(st.session_state['history'], st.session_state['inventory'])
                save_data()
                st.success("完工入庫成功")
                time.sleep(1)
                st.rerun()
                
    df = st.session_state['history']
    if not df.empty:
        mask = df['單據類型'].astype(str).str.contains('製造')
        st.dataframe(get_safe_view(df[mask]), use_container_width=True)

# ---------------------------------------------------------
# 頁面 3: 商品出貨表
# ---------------------------------------------------------
elif page == "🚚 銷售出貨 (業務/出貨)":
    st.subheader("🚚 出貨紀錄表")
    
    with st.expander("➖ 新增銷售出貨單", expanded=True):
        inv_df = st.session_state['inventory']
        inv_df['label'] = inv_df['貨號'] + " | " + inv_df['品名'] + " | 庫存:" + inv_df['庫存_成品倉'].astype(str)
        
        with st.form("sales"):
            c1, c2 = st.columns([2, 1])
            s_sel = c1.selectbox("商品", inv_df['label'].tolist())
            s_wh = c2.selectbox("出貨倉", WAREHOUSES, index=2)
            
            c3, c4, c5 = st.columns(3)
            s_qty = c3.number_input("數量", 1)
            s_fee = c4.number_input("運費", 0)
            s_date = c5.date_input("出貨日期", date.today())
            
            c6, c7 = st.columns(2)
            s_ord = c6.text_input("訂單單號")
            s_user = c7.selectbox("Key單者", DEFAULT_KEYERS)
            s_note = st.text_area("備註")
            
            if st.form_submit_button("確認出貨"):
                s_row = inv_df[inv_df['label'] == s_sel].iloc[0]
                rec = {
                    '單據類型': '銷售出貨',
                    '單號': datetime.now().strftime('%Y%m%d%H%M%S'),
                    '日期': s_date, '系列': s_row['系列'], '分類': s_row['分類'], 
                    '品名': s_row['品名'], '貨號': s_row['貨號'], '批號': '',
                    '倉庫': s_wh, '數量': s_qty, 'Key單者': s_user, 
                    '訂單單號': s_ord, '運費': s_fee, '備註': s_note
                }
                st.session_state['history'] = pd.concat([st.session_state['history'], pd.DataFrame([rec])], ignore_index=True)
                st.session_state['inventory'] = recalculate_inventory(st.session_state['history'], st.session_state['inventory'])
                save_data()
                st.success(f"出貨成功！(含運費 ${s_fee})")
                time.sleep(1)
                st.rerun()

    df = st.session_state['history']
    if not df.empty:
        mask = df['單據類型'].isin(['銷售出貨', '製造領料'])
        st.dataframe(get_safe_view(df[mask]), use_container_width=True)

# ---------------------------------------------------------
# 頁面 4: 建檔
# ---------------------------------------------------------
elif page == "📦 商品建檔與維護":
    st.subheader("📦 商品資料庫")
    with st.form("new_p"):
        c1, c2 = st.columns(2)
        name = c1.text_input("品名")
        sku = c2.text_input("貨號", value=f"P-{int(time.time())}")
        cat = st.selectbox("分類", DEFAULT_CATEGORIES)
        ser = st.selectbox("系列", DEFAULT_SERIES)
        
        if st.form_submit_button("建檔"):
            new_row = {'貨號': sku, '系列': ser, '分類': cat, '品名': name, '總庫存': 0, '均價': 0}
            for w in WAREHOUSES: new_row[f'庫存_{w}'] = 0
            st.session_state['inventory'] = pd.concat([st.session_state['inventory'], pd.DataFrame([new_row])], ignore_index=True)
            save_data()
            st.success("成功")
            time.sleep(1)
            st.rerun()
    
    st.dataframe(get_safe_view(st.session_state['inventory']))

# ---------------------------------------------------------
# 頁面 5: 財務 (加密)
# ---------------------------------------------------------
elif page == "💰 成本與財務管理 (加密)":
    st.subheader("💰 成本與財務中心")
    pwd = st.text_input("請輸入管理員密碼", type="password")
    
    if pwd == ADMIN_PASSWORD:
        st.success("身分驗證成功")
        tab_fix, tab_full, tab_inv = st.tabs(["💸 補登進貨成本", "📜 完整流水帳 (含金額)", "📊 庫存資產總表"])
        
        with tab_fix:
            df = st.session_state['history']
            mask_fix = (df['單據類型'] == '進貨') & (df['進貨總成本'] == 0)
            df_fix = df[mask_fix].copy()
            
            if df_fix.empty:
                st.info("✅ 暫無待補登單據。")
            else:
                st.markdown("#### 補登進貨成本")
                edited_fix = st.data_editor(
                    df_fix,
                    column_config={
                        "進貨總成本": st.column_config.NumberColumn("進貨總成本", required=True, format="$%d")
                    },
                    use_container_width=True
                )
                if st.button("💾 儲存"):
                    df.update(edited_fix)
                    st.session_state['history'] = df
                    st.session_state['inventory'] = recalculate_inventory(df, st.session_state['inventory'])
                    save_data()
                    st.success("已更新")
                    time.sleep(1)
                    st.rerun()

        with tab_full:
            st.write("完整流水帳 (包含所有敏感金額)")
            edited_all = st.data_editor(st.session_state['history'], use_container_width=True, num_rows="dynamic")
            if st.button("💾 儲存總表修正"):
                st.session_state['history'] = edited_all
                st.session_state['inventory'] = recalculate_inventory(edited_all, st.session_state['inventory'])
                save_data()
                st.success("已更新")

        with tab_inv:
            st.write("完整庫存表 (包含均價)")
            st.dataframe(
                st.session_state['inventory'],
                use_container_width=True,
                column_config={
                    "均價": st.column_config.NumberColumn(format="$%.2f"),
                    "總庫存": st.column_config.NumberColumn(format="%d")
                }
            )
