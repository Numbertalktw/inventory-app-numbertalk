import streamlit as st
import pandas as pd
from datetime import date, datetime
import os
import time
import io

# ==========================================
# 1. 系統設定
# ==========================================

PAGE_TITLE = "製造庫存系統 (智慧建檔版)"
INVENTORY_FILE = 'inventory_secure_v2.csv'
HISTORY_FILE = 'history_secure_v2.csv'
ADMIN_PASSWORD = "8888"  # 管理員密碼

# 倉庫 (人員)
WAREHOUSES = ["Wen", "千畇", "James", "Imeng"]

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
    '庫存_Wen', '庫存_千畇', '庫存_James', '庫存_Imeng'
]

# 預設資料 (系統會自動學習新輸入的，這裡只是初始值)
DEFAULT_SERIES = ["原料", "半成品", "成品", "包材"]
DEFAULT_CATEGORIES = ["天然石", "金屬配件", "線材", "包裝材料", "完成品"]
DEFAULT_KEYERS = ["Wen", "千畇", "James", "Imeng", "小幫手"]

# 貨號前綴對照表 (可自行擴充)
PREFIX_MAP = {
    "天然石": "ST",
    "金屬配件": "MT",
    "線材": "WR",
    "包裝材料": "PK",
    "完成品": "PD",
    "耗材": "OT"
}

# ==========================================
# 2. 核心函式
# ==========================================

def load_data():
    """讀取 CSV 資料"""
    if os.path.exists(INVENTORY_FILE):
        try:
            inv_df = pd.read_csv(INVENTORY_FILE)
            rename_map = {
                '庫存_原物料倉': '庫存_Wen', '庫存_半成品倉': '庫存_千畇',
                '庫存_成品倉': '庫存_James', '庫存_報廢倉': '庫存_Imeng'
            }
            inv_df = inv_df.rename(columns=rename_map)
            for col in INVENTORY_COLUMNS:
                if col not in inv_df.columns:
                    inv_df[col] = 0.0 if '庫存' in col or '均價' in col else ""
            inv_df['貨號'] = inv_df['貨號'].astype(str)
        except:
            inv_df = pd.DataFrame(columns=INVENTORY_COLUMNS)
    else:
        inv_df = pd.DataFrame(columns=INVENTORY_COLUMNS)

    if os.path.exists(HISTORY_FILE):
        try:
            hist_df = pd.read_csv(HISTORY_FILE)
            if '倉庫' in hist_df.columns:
                replace_map = {'原物料倉': 'Wen', '半成品倉': '千畇', '成品倉': 'James', '報廢倉': 'Imeng'}
                hist_df['倉庫'] = hist_df['倉庫'].replace(replace_map)
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
    if 'inventory' in st.session_state:
        st.session_state['inventory'].to_csv(INVENTORY_FILE, index=False, encoding='utf-8-sig')
    if 'history' in st.session_state:
        st.session_state['history'].to_csv(HISTORY_FILE, index=False, encoding='utf-8-sig')

def recalculate_inventory(hist_df, current_inv_df):
    new_inv = current_inv_df[INVENTORY_COLUMNS].copy()
    if not hist_df.empty:
        existing_skus = set(new_inv['貨號'].astype(str))
        hist_skus = set(hist_df['貨號'].astype(str))
        new_skus = hist_skus - existing_skus
        if new_skus:
            temp_df = hist_df[hist_df['貨號'].isin(new_skus)][['貨號','系列','分類','品名']].drop_duplicates('貨號')
            for col in INVENTORY_COLUMNS:
                if col not in temp_df.columns: temp_df[col] = 0.0
            new_inv = pd.concat([new_inv, temp_df], ignore_index=True)

    cols_reset = ['總庫存', '均價'] + [f'庫存_{w}' for w in WAREHOUSES]
    for col in cols_reset:
        new_inv[col] = 0.0
    
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
            w_name = str(h_row['倉庫']).strip()
            if w_name not in WAREHOUSES: w_name = "Wen"
            
            if doc_type in ['進貨', '製造入庫', '調整入庫']:
                if cost_total > 0:
                    total_value += cost_total
                total_qty += qty
                if w_name in w_stock: w_stock[w_name] += qty
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

def get_dynamic_options(column_name, default_list):
    """
    智慧選單：合併預設值 + 現有庫存中的值
    """
    options = set(default_list)
    if not st.session_state['inventory'].empty:
        existing = st.session_state['inventory'][column_name].dropna().unique().tolist()
        options.update([str(x) for x in existing if str(x).strip() != ""])
    return sorted(list(options)) + ["➕ 手動輸入新資料"]

def auto_generate_sku(category):
    """
    智慧貨號產生器：
    1. 根據分類取得前綴 (如: 天然石 -> ST)
    2. 找出目前該前綴的最大號碼
    3. 自動 +1
    """
    prefix = PREFIX_MAP.get(category, "XX") # 預設前綴 XX
    
    # 從庫存中找現有的
    df = st.session_state['inventory']
    if df.empty:
        return f"{prefix}0001"
    
    # 篩選出同系列的貨號
    # 假設貨號格式為 PREFIX + 數字 (例如 ST0005)
    # 我們嘗試用正則表達式提取數字
    
    # 1. 找出所有以該 prefix 開頭的貨號
    same_prefix = df[df['貨號'].astype(str).str.startswith(prefix)]
    
    if same_prefix.empty:
        return f"{prefix}0001"
    
    try:
        # 2. 提取數字部分 (移除前綴)
        # 用 extract 抓出數字，轉成 int
        max_num = same_prefix['貨號'].str.replace(prefix, '', regex=False).str.extract(r'(\d+)')[0].astype(float).max()
        
        if pd.isna(max_num):
            return f"{prefix}0001"
            
        next_num = int(max_num) + 1
        return f"{prefix}{next_num:04d}" # 補零至4位數
    except:
        # 如果格式太亂無法解析，就用時間戳記當備案
        return f"{prefix}-{int(time.time())}"

def process_product_upload(file_obj):
    try:
        if file_obj.name.endswith('.csv'):
            df = pd.read_csv(file_obj)
        else:
            df = pd.read_excel(file_obj)
        rename_map = {'名稱': '品名', '商品名稱': '品名', '類別': '分類', 'SKU': '貨號'}
        df = df.rename(columns=rename_map)
        if '貨號' not in df.columns or '品名' not in df.columns:
            return None, "缺少必要欄位：'貨號' 或 '品名'"
        target_cols = ['貨號', '系列', '分類', '品名']
        for col in target_cols:
            if col not in df.columns:
                df[col] = "未分類" if col != '貨號' and col != '品名' else ""
        new_products = df[target_cols].copy()
        new_products['貨號'] = new_products['貨號'].astype(str)
        return new_products, "OK"
    except Exception as e:
        return None, str(e)

def process_restore_upload(file_obj):
    try:
        df_res = pd.read_excel(file_obj, sheet_name='完整流水帳')
        for c in HISTORY_COLUMNS:
            if c not in df_res.columns: df_res[c] = ""
        df_res['數量'] = pd.to_numeric(df_res['數量'], errors='coerce').fillna(0)
        df_res['進貨總成本'] = pd.to_numeric(df_res['進貨總成本'], errors='coerce').fillna(0)
        return df_res
    except Exception as e:
        st.error(f"還原失敗: {e}")
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

st.set_page_config(page_title=PAGE_TITLE, layout="wide", page_icon="🏭")
st.title(f"🏭 {PAGE_TITLE}")

with st.sidebar:
    st.header("部門功能導航")
    page = st.radio("選擇作業", [
        "📦 商品建檔與維護", # 核心功能
        "📥 進貨庫存 (無金額)", 
        "🔨 製造生產 (工廠)", 
        "🚚 銷售出貨 (業務/出貨)", 
        "📊 總表監控 (修改/刪除)",
        "💰 成本與財務管理 (加密)"
    ])
    
    st.divider()
    st.markdown("### 💾 資料管理")
    
    if not st.session_state['history'].empty:
        excel_data = convert_to_excel_all_sheets(st.session_state['inventory'], st.session_state['history'])
        st.download_button(
            label="📥 下載完整四合一報表",
            data=excel_data,
            file_name=f'Report_{date.today()}.xlsx',
            mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )
    
    with st.expander("⚙️ 系統還原 (上傳備份)", expanded=False):
        restore_file = st.file_uploader("上傳備份檔", type=['xlsx'], key='restore')
        if restore_file and st.button("確認還原並重算"):
            df_new_hist = process_restore_upload(restore_file)
            if df_new_hist is not None:
                st.session_state['history'] = df_new_hist
                st.session_state['inventory'] = recalculate_inventory(df_new_hist, st.session_state['inventory'])
                save_data()
                st.success("還原成功！")
                time.sleep(1)
                st.rerun()

# ---------------------------------------------------------
# 頁面 1: 建檔 (含智慧下拉與自動貨號)
# ---------------------------------------------------------
if page == "📦 商品建檔與維護":
    st.subheader("📦 商品資料庫管理")
    
    tab_single, tab_batch, tab_list = st.tabs(["✨ 單筆建檔", "📂 批次匯入 (Excel)", "📋 檢視商品清單"])
    
    with tab_single:
        st.caption("在此建立新商品，貨號將根據分類自動產生，也可手動修改。")
        
        # 1. 智慧分類選單
        cat_opts = get_dynamic_options('分類', DEFAULT_CATEGORIES)
        cat_sel = st.selectbox("商品分類", cat_opts)
        
        if cat_sel == "➕ 手動輸入新資料":
            final_cat = st.text_input("↳ 請輸入新分類名稱")
        else:
            final_cat = cat_sel
            
        # 2. 智慧系列選單
        ser_opts = get_dynamic_options('系列', DEFAULT_SERIES)
        ser_sel = st.selectbox("商品系列", ser_opts)
        
        if ser_sel == "➕ 手動輸入新資料":
            final_ser = st.text_input("↳ 請輸入新系列名稱")
        else:
            final_ser = ser_sel

        # 3. 其他欄位
        name = st.text_input("商品品名")
        
        # 4. 自動貨號邏輯 (當分類確定後自動運算)
        auto_sku = auto_generate_sku(final_cat) if final_cat else ""
        sku = st.text_input("商品貨號 (預設自動產生，可修改)", value=auto_sku)
        
        if st.button("確認建立新商品", type="primary"):
            if not name:
                st.error("❌ 品名為必填欄位")
            elif not final_cat or not final_ser:
                st.error("❌ 分類與系列為必填")
            else:
                # 檢查貨號是否重複
                if not st.session_state['inventory'].empty and sku in st.session_state['inventory']['貨號'].values:
                    st.warning(f"⚠️ 貨號 {sku} 已存在！請確認是否重複建檔。")
                else:
                    new_row = {'貨號': sku, '系列': final_ser, '分類': final_cat, '品名': name, '總庫存': 0, '均價': 0}
                    for w in WAREHOUSES: new_row[f'庫存_{w}'] = 0
                    st.session_state['inventory'] = pd.concat([st.session_state['inventory'], pd.DataFrame([new_row])], ignore_index=True)
                    save_data()
                    st.success(f"✅ 已成功建立：{name} ({sku})")
                    time.sleep(1)
                    st.rerun() # 重新整理以更新下拉選單
    
    with tab_batch:
        st.info("Excel 需包含：`貨號`、`品名` (選填：`分類`、`系列`)")
        up_prod = st.file_uploader("選擇 Excel", type=['xlsx', 'xls', 'csv'], key='prod_up')
        if up_prod and st.button("開始匯入"):
            new_prods, msg = process_product_upload(up_prod)
            if new_prods is None:
                st.error(msg)
            else:
                old_inv = st.session_state['inventory'].copy()
                count_new = 0
                count_update = 0
                for _, row in new_prods.iterrows():
                    sku = str(row['貨號'])
                    mask = old_inv['貨號'] == sku
                    if mask.any():
                        idx = old_inv[mask].index[0]
                        old_inv.at[idx, '品名'] = row['品名']
                        old_inv.at[idx, '分類'] = row['分類']
                        old_inv.at[idx, '系列'] = row['系列']
                        count_update += 1
                    else:
                        new_row = row.to_dict()
                        new_row['總庫存'] = 0
                        new_row['均價'] = 0
                        for w in WAREHOUSES: new_row[f'庫存_{w}'] = 0
                        old_inv = pd.concat([old_inv, pd.DataFrame([new_row])], ignore_index=True)
                        count_new += 1
                st.session_state['inventory'] = old_inv
                save_data()
                st.success(f"匯入完成！新增 {count_new} 筆，更新 {count_update} 筆。")
                time.sleep(1)
                st.rerun()

    with tab_list:
        st.dataframe(get_safe_view(st.session_state['inventory']), use_container_width=True)

# ---------------------------------------------------------
# 頁面 2: 進貨
# ---------------------------------------------------------
elif page == "📥 進貨庫存 (無金額)":
    st.subheader("📥 進貨點收")
    with st.expander("➕ 新增進貨單", expanded=True):
        inv_df = st.session_state['inventory']
        if inv_df.empty:
            st.warning("請先至「商品建檔」建立資料")
        else:
            inv_df['label'] = inv_df['貨號'] + " | " + inv_df['品名']
            c1, c2, c3 = st.columns([2, 1, 1])
            p_sel = c1.selectbox("進貨商品", inv_df['label'].tolist())
            p_wh = c2.selectbox("入庫至 (負責人)", WAREHOUSES, index=0)
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
# 頁面 3: 製造
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
            m_wh = c2.selectbox("從誰領料", WAREHOUSES, index=0)
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
            f_wh = c2.selectbox("入庫給誰", WAREHOUSES, index=1)
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
# 頁面 4: 出貨
# ---------------------------------------------------------
elif page == "🚚 銷售出貨 (業務/出貨)":
    st.subheader("🚚 出貨紀錄表")
    with st.expander("➖ 新增銷售出貨單", expanded=True):
        inv_df = st.session_state['inventory']
        inv_df['label'] = inv_df['貨號'] + " | " + inv_df['品名'] + " | 總存:" + inv_df['總庫存'].astype(str)
        
        with st.form("sales"):
            c1, c2 = st.columns([2, 1])
            s_sel = c1.selectbox("商品", inv_df['label'].tolist())
            s_wh = c2.selectbox("從誰出貨", WAREHOUSES, index=2)
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
                st.success(f"出貨成功！")
                time.sleep(1)
                st.rerun()

    df = st.session_state['history']
    if not df.empty:
        mask = df['單據類型'].isin(['銷售出貨', '製造領料'])
        st.dataframe(get_safe_view(df[mask]), use_container_width=True)

# ---------------------------------------------------------
# 頁面 0: 總表監控 (新增)
# ---------------------------------------------------------
elif page == "📊 總表監控 (修改/刪除)":
    st.subheader("📊 總表監控與資料維護")
    st.info("此處可檢視完整資料，並進行「修改」或「刪除」。")
    
    tab_inv, tab_hist = st.tabs(["📦 庫存總表 (狀態)", "📜 完整流水帳 (可刪除/修正)"])
    
    with tab_inv:
        st.caption("各倉庫即時庫存狀況")
        df_inv = st.session_state['inventory']
        if not df_inv.empty:
            edited_inv = st.data_editor(
                df_inv,
                use_container_width=True,
                num_rows="dynamic",
                column_config={
                    "總庫存": st.column_config.NumberColumn(disabled=True),
                    "均價": st.column_config.NumberColumn(format="$%.2f", disabled=True)
                }
            )
            if st.button("💾 儲存商品資料變更"):
                st.session_state['inventory'] = edited_inv
                save_data()
                st.success("商品資料已更新")

    with tab_hist:
        st.caption("勾選左側方框可刪除整行；點擊儲存格可修改內容。")
        df_hist = st.session_state['history']
        if not df_hist.empty:
            search = st.text_input("🔍 全局搜尋", "")
            if search:
                mask = df_hist.astype(str).apply(lambda x: x.str.contains(search, case=False)).any(axis=1)
                df_display = df_hist[mask]
            else:
                df_display = df_hist
            
            edited_hist = st.data_editor(
                df_display, 
                use_container_width=True, 
                num_rows="dynamic",
                height=600,
                column_config={
                    "倉庫": st.column_config.SelectboxColumn("倉庫", options=WAREHOUSES),
                    "單據類型": st.column_config.SelectboxColumn("單據類型", options=["進貨", "銷售出貨", "製造領料", "製造入庫"])
                }
            )
            
            if st.button("💾 儲存修正並重算庫存", type="primary"):
                st.session_state['history'] = edited_hist
                st.session_state['inventory'] = recalculate_inventory(edited_hist, st.session_state['inventory'])
                save_data()
                st.success("✅ 資料已修正，庫存數量已重新校正！")
                time.sleep(1)
                st.rerun()

# ---------------------------------------------------------
# 頁面 5: 財務
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
            edited_all = st.data_editor(st.session_state['history'], use_container_width=True, num_rows="dynamic")
            if st.button("💾 儲存總表修正"):
                st.session_state['history'] = edited_all
                st.session_state['inventory'] = recalculate_inventory(edited_all, st.session_state['inventory'])
                save_data()
                st.success("已更新")

        with tab_inv:
            st.dataframe(
                st.session_state['inventory'],
                use_container_width=True,
                column_config={
                    "均價": st.column_config.NumberColumn(format="$%.2f"),
                    "總庫存": st.column_config.NumberColumn(format="%d")
                }
            )
