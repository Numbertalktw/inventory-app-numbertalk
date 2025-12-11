import streamlit as st
import pandas as pd
from pandas.api.types import (
    is_categorical_dtype,
    is_datetime64_any_dtype,
    is_numeric_dtype,
    is_object_dtype,
)
from datetime import date, datetime
import os
import time
import io

# ==========================================
# 1. 系統設定
# ==========================================

PAGE_TITLE = "製造庫存系統" 

INVENTORY_FILE = 'inventory_secure_v5.csv'
HISTORY_FILE = 'history_secure_v5.csv'
ADMIN_PASSWORD = "8888"  # 管理員/主管密碼

# 倉庫 (人員)
WAREHOUSES = ["Wen", "千畇", "James", "Imeng"]

# --- 核心流水帳 ---
HISTORY_COLUMNS = [
    '單據類型', '單號', '日期', '系列', '分類', '品名', '貨號', '批號',
    '倉庫', '數量', 'Key單者',
    '廠商', 
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

DEFAULT_SERIES = ["原料", "半成品", "成品", "包材"]
DEFAULT_CATEGORIES = ["天然石", "金屬配件", "線材", "包裝材料", "完成品"]
DEFAULT_KEYERS = ["Wen", "千畇", "James", "Imeng", "小幫手"]

PREFIX_MAP = {
    "天然石": "ST", "金屬配件": "MT", "線材": "WR",
    "包裝材料": "PK", "完成品": "PD", "耗材": "OT"
}

# ==========================================
# 2. 核心函式
# ==========================================

def filter_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    通用篩選器 UI 元件
    讓使用者可以針對 DataFrame 的任意欄位進行篩選
    """
    modify = st.checkbox("🔍 開啟資料篩選器 (Filter Data)")

    if not modify:
        return df

    df = df.copy()

    # 嘗試轉換日期欄位格式以便篩選
    for col in df.columns:
        if is_object_dtype(df[col]):
            try:
                df[col] = pd.to_datetime(df[col])
            except Exception:
                pass

    modification_container = st.container()

    with modification_container:
        to_filter_columns = st.multiselect("選擇要篩選的欄位", df.columns)
        
        for column in to_filter_columns:
            left, right = st.columns((1, 20))
            left.write("↳")
            
            # 處理各種資料類型的篩選邏輯
            if is_categorical_dtype(df[column]) or df[column].nunique() < 20:
                # 如果選項少，用多選選單
                user_cat_input = right.multiselect(
                    f"選擇 {column} 的內容",
                    df[column].unique(),
                    default=list(df[column].unique()),
                )
                df = df[df[column].isin(user_cat_input)]
                
            elif is_numeric_dtype(df[column]):
                # 如果是數字，用範圍滑桿
                _min = float(df[column].min())
                _max = float(df[column].max())
                step = (_max - _min) / 100
                user_num_input = right.slider(
                    f"設定 {column} 的範圍",
                    min_value=_min,
                    max_value=_max,
                    value=(_min, _max),
                    step=step,
                )
                df = df[df[column].between(*user_num_input)]
                
            elif is_datetime64_any_dtype(df[column]):
                # 如果是日期，用日期選擇器
                user_date_input = right.date_input(
                    f"選擇 {column} 的範圍",
                    value=(
                        df[column].min(),
                        df[column].max(),
                    ),
                )
                if len(user_date_input) == 2:
                    user_date_input = tuple(map(pd.to_datetime, user_date_input))
                    start_date, end_date = user_date_input
                    df = df.loc[df[column] >= start_date]
                    df = df.loc[df[column] <= end_date]
                    
            else:
                # 其他文字，用關鍵字搜尋
                user_text_input = right.text_input(
                    f"搜尋 {column} 包含的字串",
                )
                if user_text_input:
                    df = df[df[column].astype(str).str.contains(user_text_input, case=False)]

    return df

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
            
            if doc_type in ['進貨', '製造入庫', '調整入庫', '期初建檔', '庫存調整(加)']:
                if cost_total > 0:
                    total_value += cost_total
                total_qty += qty
                if w_name in w_stock: w_stock[w_name] += qty
            elif doc_type in ['銷售出貨', '製造領料', '調整出庫', '庫存調整(減)']:
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

def gen_mo_number():
    return f"MO-{datetime.now().strftime('%y%m%d-%H%M')}"

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

def convert_single_sheet_to_excel(df, sheet_name="Sheet1"):
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name=sheet_name)
    return output.getvalue()

def get_dynamic_options(column_name, default_list):
    options = set(default_list)
    if not st.session_state['inventory'].empty:
        existing = st.session_state['inventory'][column_name].dropna().unique().tolist()
        options.update([str(x) for x in existing if str(x).strip() != ""])
    return sorted(list(options)) + ["➕ 手動輸入新資料"]

def auto_generate_sku(category):
    prefix = PREFIX_MAP.get(category, "XX")
    df = st.session_state['inventory']
    if df.empty: return f"{prefix}0001"
    same_prefix = df[df['貨號'].astype(str).str.startswith(prefix)]
    if same_prefix.empty: return f"{prefix}0001"
    try:
        max_num = same_prefix['貨號'].str.replace(prefix, '', regex=False).str.extract(r'(\d+)')[0].astype(float).max()
        if pd.isna(max_num): return f"{prefix}0001"
        next_num = int(max_num) + 1
        return f"{prefix}{next_num:04d}"
    except:
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

def process_opening_stock_upload(file_obj, default_warehouse):
    try:
        if file_obj.name.endswith('.csv'):
            df = pd.read_csv(file_obj)
        else:
            df = pd.read_excel(file_obj)
        
        rename_map = {
            '名稱': '品名', '商品名稱': '品名', 'SKU': '貨號', 
            '庫存': '數量', '現有庫存': '數量', '成本': '進貨總成本', '總成本': '進貨總成本'
        }
        df = df.rename(columns=rename_map)
        
        if '貨號' not in df.columns or '數量' not in df.columns:
            return None, "Excel 必須包含「貨號」與「數量」欄位"
            
        new_records = []
        batch_no = f"INIT-{date.today().strftime('%Y%m%d')}"
        
        for _, row in df.iterrows():
            sku = str(row['貨號'])
            qty = pd.to_numeric(row['數量'], errors='coerce')
            if pd.isna(qty) or qty <= 0: continue
            
            wh = row['倉庫'] if '倉庫' in df.columns and pd.notna(row['倉庫']) else default_warehouse
            cost = pd.to_numeric(row['進貨總成本'], errors='coerce') if '進貨總成本' in df.columns else 0
            
            inv_ref = st.session_state['inventory']
            ref_row = inv_ref[inv_ref['貨號'] == sku]
            
            if not ref_row.empty:
                series = ref_row.iloc[0]['系列']
                category = ref_row.iloc[0]['分類']
                name = ref_row.iloc[0]['品名']
            else:
                series = row.get('系列', '期初匯入')
                category = row.get('分類', '期初匯入')
                name = row.get('品名', f'未知品名-{sku}')

            rec = {
                '單據類型': '期初建檔',
                '單號': f"OPEN-{int(time.time())}-{sku}",
                '日期': date.today(),
                '系列': series, '分類': category, '品名': name, '貨號': sku,
                '批號': batch_no,
                '倉庫': wh,
                '數量': qty,
                'Key單者': '系統匯入',
                '進貨總成本': cost,
                '備註': 'Excel期初庫存匯入'
            }
            for c in HISTORY_COLUMNS:
                if c not in rec: rec[c] = ""
            new_records.append(rec)
            
        return pd.DataFrame(new_records), "OK"

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
        "📦 商品建檔與維護", 
        "⚖️ 庫存盤點與調整",
        "📥 進貨庫存 (無金額)", 
        "🔨 製造生產 (工廠)", 
        "🚚 銷售出貨 (業務/出貨)", 
        "📊 總表監控 (主管專用)",
        "💰 成本與財務管理 (加密)"
    ])
    
    st.divider()
    st.markdown("### 💾 資料管理")
    
    if not st.session_state['history'].empty:
        with st.expander("📥 下載單獨報表", expanded=False):
            st.download_button("📊 庫存現況表.xlsx",
                data=convert_single_sheet_to_excel(st.session_state['inventory'], "庫存表"),
                file_name=f"Stock_{date.today()}.xlsx")
            
            df_in = st.session_state['history'][st.session_state['history']['單據類型'] == '進貨']
            st.download_button("📥 進貨紀錄表.xlsx",
                data=convert_single_sheet_to_excel(df_in, "進貨紀錄"),
                file_name=f"Purchase_{date.today()}.xlsx")
                
            df_out = st.session_state['history'][st.session_state['history']['單據類型'].isin(['銷售出貨'])]
            st.download_button("🚚 銷貨紀錄表.xlsx",
                data=convert_single_sheet_to_excel(df_out, "銷貨紀錄"),
                file_name=f"Sales_{date.today()}.xlsx")
                
            df_mfg = st.session_state['history'][st.session_state['history']['單據類型'].str.contains('製造', na=False)]
            st.download_button("🔨 製造紀錄表.xlsx",
                data=convert_single_sheet_to_excel(df_mfg, "製造紀錄"),
                file_name=f"Mfg_{date.today()}.xlsx")

        excel_data = convert_to_excel_all_sheets(st.session_state['inventory'], st.session_state['history'])
        st.download_button(
            label="📥 下載完整總表 (Excel)",
            data=excel_data,
            file_name=f'Report_Full_{date.today()}.xlsx',
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
# 頁面 1: 建檔
# ---------------------------------------------------------
if page == "📦 商品建檔與維護":
    st.subheader("📦 商品資料庫管理")
    tab_single, tab_batch, tab_opening, tab_list = st.tabs(["✨ 單筆建檔", "📂 批次匯入 (基本資料)", "📥 匯入期初庫存", "📋 檢視/修改商品"])
    
    with tab_single:
        st.caption("智慧建檔：自動學習分類、自動產生貨號。")
        cat_opts = get_dynamic_options('分類', DEFAULT_CATEGORIES)
        cat_sel = st.selectbox("商品分類", cat_opts)
        final_cat = st.text_input("↳ 請輸入新分類名稱") if cat_sel == "➕ 手動輸入新資料" else cat_sel
        ser_opts = get_dynamic_options('系列', DEFAULT_SERIES)
        ser_sel = st.selectbox("商品系列", ser_opts)
        final_ser = st.text_input("↳ 請輸入新系列名稱") if ser_sel == "➕ 手動輸入新資料" else ser_sel
        name = st.text_input("商品品名")
        auto_sku = auto_generate_sku(final_cat) if final_cat else ""
        sku = st.text_input("商品貨號 (預設自動產生)", value=auto_sku)
        
        if st.button("確認建立新商品", type="primary"):
            if not name or not final_cat or not final_ser:
                st.error("品名、分類、系列為必填")
            else:
                if not st.session_state['inventory'].empty and sku in st.session_state['inventory']['貨號'].values:
                    st.warning(f"⚠️ 貨號 {sku} 已存在")
                else:
                    new_row = {'貨號': sku, '系列': final_ser, '分類': final_cat, '品名': name, '總庫存': 0, '均價': 0}
                    for w in WAREHOUSES: new_row[f'庫存_{w}'] = 0
                    st.session_state['inventory'] = pd.concat([st.session_state['inventory'], pd.DataFrame([new_row])], ignore_index=True)
                    save_data()
                    st.success(f"✅ 已建立：{name} ({sku})")
                    time.sleep(1)
                    st.rerun()
    
    with tab_batch:
        st.info("僅匯入商品資料 (貨號、品名、分類)，不影響庫存數量。")
        up_prod = st.file_uploader("選擇 Excel", type=['xlsx', 'xls', 'csv'], key='prod_up')
        if up_prod and st.button("開始匯入商品資料"):
            new_prods, msg = process_product_upload(up_prod)
            if new_prods is None:
                st.error(msg)
            else:
                old_inv = st.session_state['inventory'].copy()
                for _, row in new_prods.iterrows():
                    sku = str(row['貨號'])
                    mask = old_inv['貨號'] == sku
                    if mask.any():
                        idx = old_inv[mask].index[0]
                        old_inv.at[idx, '品名'] = row['品名']
                        old_inv.at[idx, '分類'] = row['分類']
                        old_inv.at[idx, '系列'] = row['系列']
                    else:
                        new_row = row.to_dict()
                        new_row['總庫存'] = 0
                        new_row['均價'] = 0
                        for w in WAREHOUSES: new_row[f'庫存_{w}'] = 0
                        old_inv = pd.concat([old_inv, pd.DataFrame([new_row])], ignore_index=True)
                st.session_state['inventory'] = old_inv
                save_data()
                st.success("匯入完成！")
                time.sleep(1)
                st.rerun()

    with tab_opening:
        st.markdown("### 📥 匯入現有庫存 (Excel)")
        target_wh = st.selectbox("若 Excel 無倉庫欄位，預設匯入至：", WAREHOUSES)
        up_stock = st.file_uploader("上傳庫存盤點表", type=['xlsx', 'xls', 'csv'], key='stock_up')
        if up_stock and st.button("確認匯入庫存"):
            df_opening_hist, msg = process_opening_stock_upload(up_stock, target_wh)
            if df_opening_hist is None:
                st.error(msg)
            elif df_opening_hist.empty:
                st.warning("無效庫存資料")
            else:
                st.session_state['history'] = pd.concat([st.session_state['history'], df_opening_hist], ignore_index=True)
                st.session_state['inventory'] = recalculate_inventory(st.session_state['history'], st.session_state['inventory'])
                save_data()
                st.success(f"✅ 成功匯入 {len(df_opening_hist)} 筆庫存資料！")
                time.sleep(1)
                st.rerun()

    with tab_list:
        st.info("此處可直接修改品名、分類或系列。修改後請務必按下「儲存修改」按鈕。")
        df_safe = get_safe_view(st.session_state['inventory'])
        
        # ★★★ 加入篩選功能 ★★★
        df_safe = filter_dataframe(df_safe)
        
        edited_products = st.data_editor(
            df_safe,
            use_container_width=True,
            num_rows="dynamic",
            column_config={
                "貨號": st.column_config.TextColumn(disabled=True),
                "總庫存": st.column_config.NumberColumn(disabled=True),
                "庫存_Wen": st.column_config.NumberColumn(disabled=True),
                "庫存_千畇": st.column_config.NumberColumn(disabled=True),
                "庫存_James": st.column_config.NumberColumn(disabled=True),
                "庫存_Imeng": st.column_config.NumberColumn(disabled=True)
            }
        )
        if st.button("💾 儲存商品資料修改"):
            current_inv = st.session_state['inventory']
            for idx, row in edited_products.iterrows():
                if idx in current_inv.index:
                    current_inv.at[idx, '品名'] = row['品名']
                    current_inv.at[idx, '分類'] = row['分類']
                    current_inv.at[idx, '系列'] = row['系列']
            st.session_state['inventory'] = current_inv
            save_data()
            st.success("✅ 商品資料已更新！")

# ---------------------------------------------------------
# 頁面 X: 庫存盤點與調整
# ---------------------------------------------------------
elif page == "⚖️ 庫存盤點與調整":
    st.subheader("⚖️ 快速修正庫存 (盤點調整)")
    inv_df = st.session_state['inventory']
    if inv_df.empty:
        st.warning("無商品資料")
    else:
        inv_df['label'] = inv_df['貨號'] + " | " + inv_df['品名']
        
        c1, c2 = st.columns([2, 1])
        with c1:
            sel_item = st.selectbox("選擇要調整的商品", inv_df['label'].tolist())
            row = inv_df[inv_df['label'] == sel_item].iloc[0]
        with c2:
            sel_wh = st.selectbox("調整哪個倉庫的庫存？", WAREHOUSES)
            
        curr_qty = row[f'庫存_{sel_wh}']
        st.metric(f"目前 {sel_wh} 系統庫存", f"{int(curr_qty)}")
        
        st.divider()
        
        with st.form("adj_form"):
            new_qty = st.number_input("🔴 請輸入正確的【盤點實際數量】", min_value=0, value=int(curr_qty))
            adj_reason = st.text_input("調整原因 (例如：盤點差異、遺失、破損)", value="庫存盤點修正")
            
            if st.form_submit_button("✅ 確認修正庫存"):
                diff = new_qty - curr_qty
                
                if diff == 0:
                    st.warning("數量未變動，無需調整。")
                else:
                    action = "庫存調整(加)" if diff > 0 else "庫存調整(減)"
                    final_qty = abs(diff) 
                    
                    rec = {
                        '單據類型': action,
                        '單號': f"ADJ-{int(time.time())}",
                        '日期': date.today(),
                        '系列': row['系列'], '分類': row['分類'], '品名': row['品名'], '貨號': row['貨號'],
                        '批號': '',
                        '倉庫': sel_wh,
                        '數量': final_qty,
                        'Key單者': '盤點調整',
                        '備註': f"{adj_reason} (原:{int(curr_qty)} -> 新:{int(new_qty)})"
                    }
                    
                    st.session_state['history'] = pd.concat([st.session_state['history'], pd.DataFrame([rec])], ignore_index=True)
                    st.session_state['inventory'] = recalculate_inventory(st.session_state['history'], st.session_state['inventory'])
                    save_data()
                    st.success(f"已修正！庫存已更新為 {new_qty}。")
                    time.sleep(1)
                    st.rerun()

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
            p_wh = c2.selectbox("入庫倉庫", WAREHOUSES, index=0)
            p_qty = c3.number_input("進貨數量", 1)
            
            c4, c5, c6 = st.columns(3)
            p_date = c4.date_input("進貨日期", date.today())
            p_user = c5.selectbox("Key單者", DEFAULT_KEYERS)
            p_sup = c6.text_input("廠商名稱 (Supplier)")
            p_note = st.text_input("備註")
            
            if st.button("確認進貨"):
                p_row = inv_df[inv_df['label'] == p_sel].iloc[0]
                rec = {
                    '單據類型': '進貨',
                    '單號': datetime.now().strftime('%Y%m%d%H%M%S'),
                    '日期': p_date, '系列': p_row['系列'], '分類': p_row['分類'], 
                    '品名': p_row['品名'], '貨號': p_row['貨號'], '批號': gen_batch_number("IN"),
                    '倉庫': p_wh, '數量': p_qty, 'Key單者': p_user, '廠商': p_sup, 
                    '備註': p_note, '進貨總成本': 0
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
        purchase_cols = ['單號', '日期', '廠商', '系列', '分類', '品名', '貨號', '批號', '倉庫', '數量', 'Key單者', '備註']
        valid_cols = [c for c in purchase_cols if c in df_view.columns]
        
        # ★★★ 加入篩選功能 ★★★
        st.write("---")
        df_filtered = filter_dataframe(df_view[valid_cols])
        st.dataframe(df_filtered, use_container_width=True)

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
            c_date, c_mo = st.columns(2)
            m_date = c_date.date_input("領料日期", value=date.today())
            m_mo = c_mo.text_input("工單單號", value=gen_mo_number())
            c1, c2 = st.columns([2, 1])
            m_sel = c1.selectbox("原料", inv_df['label'].tolist())
            m_wh = c2.selectbox("從誰領料", WAREHOUSES, index=0)
            c3, c4 = st.columns(2)
            m_qty = c3.number_input("領用量", 1)
            m_user = c4.selectbox("領料人", DEFAULT_KEYERS)
            if st.form_submit_button("確認領料"):
                m_row = inv_df[inv_df['label'] == m_sel].iloc[0]
                rec = {
                    '單據類型': '製造領料',
                    '單號': datetime.now().strftime('%Y%m%d%H%M%S'),
                    '日期': m_date, '系列': m_row['系列'], '分類': m_row['分類'], 
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
            c_date, c_mo = st.columns(2)
            f_date = c_date.date_input("完工日期", value=date.today())
            f_mo = c_mo.text_input("工單單號", value=gen_mo_number())
            c1, c2 = st.columns([2, 1])
            f_sel = c1.selectbox("成品", inv_df['label'].tolist())
            f_wh = c2.selectbox("入庫給誰", WAREHOUSES, index=1)
            c3, c4, c5 = st.columns(3)
            f_qty = c3.number_input("產出量", 1)
            f_batch = c4.text_input("成品批號", value=gen_batch_number("PD"))
            f_user = c5.selectbox("Key單者", DEFAULT_KEYERS)
            if st.form_submit_button("完工入庫"):
                f_row = inv_df[inv_df['label'] == f_sel].iloc[0]
                rec = {
                    '單據類型': '製造入庫',
                    '單號': datetime.now().strftime('%Y%m%d%H%M%S'),
                    '日期': f_date, '系列': f_row['系列'], '分類': f_row['分類'], 
                    '品名': f_row['品名'], '貨號': f_row['貨號'], '批號': f_batch,
                    '倉庫': f_wh, '數量': f_qty, 'Key單者': f_user, '訂單單號': f_mo
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
        df_view = get_safe_view(df[mask])
        
        # ★★★ 加入篩選功能 ★★★
        st.write("---")
        df_filtered = filter_dataframe(df_view)
        st.dataframe(df_filtered, use_container_width=True)

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
        df_view = df[mask].copy()
        sales_cols = ['單號', '訂單單號', '出貨日期', '系列', '分類', '品名', '貨號', '倉庫', '數量', '運費', 'Key單者', '備註']
        valid_cols = [c for c in sales_cols if c in df_view.columns]
        
        # ★★★ 加入篩選功能 ★★★
        st.write("---")
        df_filtered = filter_dataframe(df_view[valid_cols])
        st.dataframe(df_filtered, use_container_width=True)

# ---------------------------------------------------------
# 頁面 0: 總表監控
# ---------------------------------------------------------
elif page == "📊 總表監控 (主管專用)":
    st.subheader("📊 總表監控與資料維護")
    st.info("此區僅供主管進入，進行資料修改或刪除。")
    pwd = st.text_input("🔒 請輸入主管密碼", type="password", key="admin_pwd")
    if pwd == ADMIN_PASSWORD:
        st.success("✅ 驗證成功")
        tab_inv, tab_hist = st.tabs(["📦 庫存總表 (狀態)", "📜 完整流水帳 (可刪除/修正)"])
        
        with tab_inv:
            df_inv = st.session_state['inventory']
            if not df_inv.empty:
                # ★★★ 加入篩選功能 ★★★
                df_filtered_inv = filter_dataframe(df_inv)
                
                edited_inv = st.data_editor(
                    df_filtered_inv, use_container_width=True, num_rows="dynamic",
                    column_config={"總庫存": st.column_config.NumberColumn(disabled=True)}
                )
                if st.button("💾 儲存商品資料變更"):
                    st.session_state['inventory'] = edited_inv
                    save_data()
                    st.success("商品資料已更新")

        with tab_hist:
            df_hist = st.session_state['history']
            if not df_hist.empty:
                # ★★★ 加入篩選功能 ★★★
                df_filtered_hist = filter_dataframe(df_hist)
                
                edited_hist = st.data_editor(
                    df_filtered_hist, use_container_width=True, num_rows="dynamic", height=600,
                    column_config={
                        "倉庫": st.column_config.SelectboxColumn("倉庫", options=WAREHOUSES),
                        "單據類型": st.column_config.SelectboxColumn("單據類型", options=["進貨", "銷售出貨", "製造領料", "製造入庫", "期初建檔", "庫存調整(加)", "庫存調整(減)"])
                    }
                )
                
                if st.button("💾 儲存修正並重算"):
                    st.session_state['history'] = edited_hist
                    st.session_state['inventory'] = recalculate_inventory(edited_hist, st.session_state['inventory'])
                    save_data()
                    st.success("已修正")
    elif pwd != "":
        st.error("密碼錯誤")

# ---------------------------------------------------------
# 頁面 5: 財務
# ---------------------------------------------------------
elif page == "💰 成本與財務管理 (加密)":
    st.subheader("💰 成本與財務中心")
    pwd = st.text_input("請輸入管理員密碼", type="password")
    
    if pwd == ADMIN_PASSWORD:
        st.success("身分驗證成功")
        tab_fix, tab_full = st.tabs(["💸 補登進貨成本", "📜 完整流水帳 (含金額)"])
        
        with tab_fix:
            df = st.session_state['history']
            mask = (df['單據類型'] == '進貨') & (df['進貨總成本'] == 0)
            df_fix = df[mask].copy()
            if df_fix.empty:
                st.info("✅ 無待補登單據")
            else:
                # ★★★ 加入篩選功能 ★★★
                df_fix_filtered = filter_dataframe(df_fix)
                
                edited = st.data_editor(df_fix_filtered, column_config={"進貨總成本": st.column_config.NumberColumn(required=True)})
                if st.button("💾 儲存"):
                    df.update(edited)
                    st.session_state['history'] = df
                    st.session_state['inventory'] = recalculate_inventory(df, st.session_state['inventory'])
                    save_data()
                    st.success("已更新")

        with tab_full:
            # ★★★ 加入篩選功能 ★★★
            df_all_filtered = filter_dataframe(st.session_state['history'])
            
            edited_all = st.data_editor(df_all_filtered, use_container_width=True, num_rows="dynamic")
            if st.button("💾 儲存修正"):
                st.session_state['history'] = edited_all
                st.session_state['inventory'] = recalculate_inventory(edited_all, st.session_state['inventory'])
                save_data()
                st.success("已更新")
