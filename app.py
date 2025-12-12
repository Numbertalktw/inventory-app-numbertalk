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
import re

# ==========================================
# 1. 系統設定
# ==========================================

PAGE_TITLE = "製造庫存系統" 

INVENTORY_FILE = 'inventory_secure_v16.csv'
HISTORY_FILE = 'history_secure_v16.csv'
RULES_FILE = 'sku_rules_composite_v2.xlsx' 
ADMIN_PASSWORD = "8888"

WAREHOUSES = ["Wen", "千畇", "James", "Imeng"]

# --- 核心流水帳 ---
HISTORY_COLUMNS = [
    '單據類型', '單號', '日期', 
    '系列', '分類', '品名', '規格', 
    '貨號', '批號',
    '倉庫', '數量', 'Key單者',
    '廠商', 
    '訂單單號', '出貨日期', '貨號備註', '運費', 
    '款項結清', '工資', '發票', '備註',
    '進貨總成本' 
]

# --- 庫存狀態表 ---
INVENTORY_COLUMNS = [
    '系列', '分類', '品名', '規格', '貨號', 
    '總庫存', '均價', 
    '庫存_Wen', '庫存_千畇', '庫存_James', '庫存_Imeng'
]

# ★★★ 修改：全部清空，強制只讀取 Excel 規則 ★★★
DEFAULT_SERIES = [] 
DEFAULT_CATEGORIES = [] 
DEFAULT_KEYERS = ["Wen", "千畇", "James", "Imeng", "小幫手"]

# ==========================================
# 2. 核心函式
# ==========================================

def safe_float(value):
    try:
        if pd.isna(value) or str(value).strip() == "": return 0.0
        return float(str(value).replace(",", ""))
    except: return 0.0

def get_safe_view(df):
    sensitive_cols = ['進貨總成本', '均價', '工資', '款項結清']
    safe_cols = [c for c in df.columns if c not in sensitive_cols]
    return df[safe_cols]

def sort_inventory(df):
    if df.empty: return df
    sort_keys = [col for col in ['系列', '分類', '品名', '規格'] if col in df.columns]
    if sort_keys:
        temp_df = df.copy()
        for k in sort_keys:
            temp_df[k] = temp_df[k].fillna("")
        return temp_df.sort_values(by=sort_keys, ascending=True).reset_index(drop=True)
    return df

def filter_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty: return df
    modify = st.checkbox("🔍 開啟資料篩選器 (Filter Data)", key=f"f_{len(df)}")
    if not modify: return df
    df = df.copy()
    for col in df.columns:
        if is_object_dtype(df[col]):
            try: df[col] = pd.to_datetime(df[col])
            except: pass
    with st.container():
        cols = st.multiselect("選擇篩選欄位", df.columns)
        for col in cols:
            if is_categorical_dtype(df[col]) or df[col].nunique() < 50:
                opts = sorted(df[col].astype(str).unique())
                if st.checkbox(f"全選 {col}", value=True, key=f"all_{col}"):
                    sel = opts
                else:
                    sel = st.multiselect(f"選擇 {col}", opts)
                if sel: df = df[df[col].astype(str).isin(sel)]
            elif is_numeric_dtype(df[col]):
                _min, _max = float(df[col].min()), float(df[col].max())
                step = (_max - _min) / 100 if _max!=_min else 0.1
                r = st.slider(f"{col} 範圍", _min, _max, (_min, _max), step=step)
                df = df[df[col].between(*r)]
            else:
                txt = st.text_input(f"搜尋 {col}")
                if txt: df = df[df[col].astype(str).str.contains(txt, case=False)]
    return df

def load_data():
    if os.path.exists(INVENTORY_FILE):
        try:
            inv_df = pd.read_csv(INVENTORY_FILE)
            rename_map = {'庫存_原物料倉': '庫存_Wen', '庫存_半成品倉': '庫存_千畇', '庫存_成品倉': '庫存_James', '庫存_報廢倉': '庫存_Imeng'}
            inv_df = inv_df.rename(columns=rename_map)
            for col in INVENTORY_COLUMNS:
                if col not in inv_df.columns:
                    inv_df[col] = 0.0 if '庫存' in col or '均價' in col else ""
            inv_df['貨號'] = inv_df['貨號'].astype(str)
            inv_df = sort_inventory(inv_df)
        except: inv_df = pd.DataFrame(columns=INVENTORY_COLUMNS)
    else: inv_df = pd.DataFrame(columns=INVENTORY_COLUMNS)

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
        except: hist_df = pd.DataFrame(columns=HISTORY_COLUMNS)
    else: hist_df = pd.DataFrame(columns=HISTORY_COLUMNS)
    return inv_df, hist_df

def load_rules():
    empty_rules = {
        'category': pd.DataFrame(columns=['名稱', '代碼']),
        'series': pd.DataFrame(columns=['名稱', '代碼']),
        'name': pd.DataFrame(columns=['名稱', '代碼']),
        'spec': pd.DataFrame(columns=['名稱', '代碼'])
    }
    if os.path.exists(RULES_FILE):
        try:
            xls = pd.ExcelFile(RULES_FILE)
            rules = {}
            sheet_map_raw = {s.strip(): s for s in xls.sheet_names}
            target_map = {'類別規則': 'category', '系列規則': 'series', '品名規則': 'name', '規格規則': 'spec'}
            for target_name, key in target_map.items():
                if target_name in sheet_map_raw:
                    real_name = sheet_map_raw[target_name]
                    df = pd.read_excel(xls, sheet_name=real_name).astype(str)
                    if df.shape[1] >= 2:
                        df = df.iloc[:, :2]
                        df.columns = ['名稱', '代碼']
                        rules[key] = df
                    else: rules[key] = empty_rules[key]
                else: rules[key] = empty_rules[key]
            return rules
        except: return empty_rules
    else: return empty_rules

def save_data():
    if 'inventory' in st.session_state:
        sorted_inv = sort_inventory(st.session_state['inventory'])
        sorted_inv.to_csv(INVENTORY_FILE, index=False, encoding='utf-8-sig')
    if 'history' in st.session_state:
        st.session_state['history'].to_csv(HISTORY_FILE, index=False, encoding='utf-8-sig')

def save_rules_to_excel(rules_dict):
    with pd.ExcelWriter(RULES_FILE, engine='openpyxl') as writer:
        name_map = {'category': '類別規則', 'series': '系列規則', 'name': '品名規則', 'spec': '規格規則'}
        for key, df in rules_dict.items():
            sheet_name = name_map.get(key, key)
            df.to_excel(writer, index=False, sheet_name=sheet_name)

def recalculate_inventory(hist_df, current_inv_df):
    new_inv = current_inv_df[INVENTORY_COLUMNS].copy()
    if not hist_df.empty:
        existing_skus = set(new_inv['貨號'].astype(str))
        hist_skus = set(hist_df['貨號'].astype(str))
        new_skus = hist_skus - existing_skus
        if new_skus:
            temp_df = hist_df[hist_df['貨號'].isin(new_skus)][['貨號','系列','分類','品名','規格']].drop_duplicates('貨號')
            for col in INVENTORY_COLUMNS:
                if col not in temp_df.columns: temp_df[col] = 0.0
            new_inv = pd.concat([new_inv, temp_df], ignore_index=True)

    cols_reset = ['總庫存', '均價'] + [f'庫存_{w}' for w in WAREHOUSES]
    for col in cols_reset: new_inv[col] = 0.0
    
    for idx, row in new_inv.iterrows():
        sku = str(row['貨號'])
        target_hist = hist_df[hist_df['貨號'].astype(str) == sku]
        total_qty = 0
        total_value = 0.0
        w_stock = {w: 0 for w in WAREHOUSES}
        for _, h_row in target_hist.iterrows():
            qty = safe_float(h_row['數量'])
            cost_total = safe_float(h_row['進貨總成本'])
            doc_type = str(h_row['單據類型'])
            w_name = str(h_row['倉庫']).strip()
            if w_name not in WAREHOUSES: w_name = "Wen"
            
            if doc_type in ['進貨', '製造入庫', '調整入庫', '期初建檔', '庫存調整(加)']:
                if cost_total > 0: total_value += cost_total
                total_qty += qty
                if w_name in w_stock: w_stock[w_name] += qty
            elif doc_type in ['銷售出貨', '製造領料', '調整出庫', '庫存調整(減)']:
                avg = (total_value / total_qty) if total_qty > 0 else 0
                total_qty -= qty
                total_value -= (qty * avg)
                if w_name in w_stock: w_stock[w_name] -= qty

        new_inv.at[idx, '總庫存'] = total_qty
        new_inv.at[idx, '均價'] = (total_value / total_qty) if total_qty > 0 else 0
        for w in WAREHOUSES: new_inv.at[idx, f'庫存_{w}'] = w_stock[w]
            
    return sort_inventory(new_inv)

def gen_batch_number(prefix="BAT"): return f"{prefix}-{datetime.now().strftime('%y%m%d%H%M')}"
def gen_mo_number(): return f"MO-{datetime.now().strftime('%y%m%d-%H%M')}"

def convert_single_sheet_to_excel(df, sheet_name="Sheet1"):
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name=sheet_name)
    return output.getvalue()

def convert_to_excel_all_sheets(inv_df, hist_df):
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        inv_df.to_excel(writer, index=False, sheet_name='庫存總表')
        if '單據類型' in hist_df.columns:
            hist_df[hist_df['單據類型'] == '進貨'].to_excel(writer, index=False, sheet_name='進貨紀錄')
            hist_df[hist_df['單據類型'].str.contains('製造', na=False)].to_excel(writer, index=False, sheet_name='製造紀錄')
            hist_df[hist_df['單據類型'].isin(['銷售出貨'])].to_excel(writer, index=False, sheet_name='出貨紀錄')
        hist_df.to_excel(writer, index=False, sheet_name='完整流水帳')
    return output.getvalue()

def get_dynamic_options(column_name, default_list):
    """
    [修改版] 僅從規則表讀取選項，不讀取舊資料庫存
    """
    options = set(default_list)
    
    # ★★★ 修改：註解掉讀取現有庫存的邏輯，避免舊資料干擾 ★★★
    # if not st.session_state['inventory'].empty:
    #     existing = st.session_state['inventory'][column_name].dropna().unique().tolist()
    #     options.update([str(x) for x in existing if str(x).strip() != ""])
        
    rules = st.session_state.get('sku_rules', {})
    rule_key_map = {'系列': 'series', '分類': 'category'}
    if column_name in rule_key_map:
        rule_key = rule_key_map[column_name]
        if rule_key in rules and not rules[rule_key].empty:
            rule_opts = rules[rule_key]['名稱'].astype(str).unique().tolist()
            options.update([x for x in rule_opts if x.strip() != ""])

    return sorted(list(options)) + ["➕ 手動輸入新資料"]

def auto_generate_composite_sku(cat, ser, name, spec):
    rules = st.session_state['sku_rules']
    def get_code(rule_key, val):
        df = rules.get(rule_key)
        if df is None or df.empty: return "XX"
        match = df[df['名稱'] == val]
        if not match.empty:
            return str(match.iloc[0]['代碼']).strip().upper()
        for _, r in df.iterrows():
            if str(r['名稱']) in str(val):
                return str(r['代碼']).strip().upper()
        return "XX"

    c_code = get_code('category', cat)
    s_code = get_code('series', ser)
    n_code = get_code('name', name)
    if n_code == "XX" and name: n_code = name[:2].upper()
    sp_code = get_code('spec', spec)
    if sp_code == "XX" and spec: 
        nums = re.findall(r'\d+', spec)
        if nums: sp_code = nums[0]
        else: sp_code = spec[:2].upper()

    return f"{c_code}-{s_code}-{n_code}-{sp_code}"

def process_rules_upload_v2(file_obj):
    try:
        xls = pd.ExcelFile(file_obj)
        sheet_map_raw = {s.strip(): s for s in xls.sheet_names}
        required_map = {'類別規則': 'category', '系列規則': 'series', '品名規則': 'name', '規格規則': 'spec'}
        new_rules = {}
        found_info = []
        for req_name, key in required_map.items():
            if req_name in sheet_map_raw:
                df = pd.read_excel(xls, sheet_name=sheet_map_raw[req_name]).astype(str)
                if df.shape[1] >= 2:
                    df = df.iloc[:, :2]
                    df.columns = ['名稱', '代碼']
                    new_rules[key] = df
                    found_info.append(f"✅ {req_name}")
                else:
                    new_rules[key] = pd.DataFrame(columns=['名稱', '代碼'])
            else:
                new_rules[key] = pd.DataFrame(columns=['名稱', '代碼'])
        return new_rules, " / ".join(found_info)
    except Exception as e: return None, str(e)

def process_product_upload(file):
    try:
        df = pd.read_csv(file) if file.name.endswith('.csv') else pd.read_excel(file)
        rename = {'名稱':'品名','商品名稱':'品名','SKU':'貨號','類別':'分類'}
        df = df.rename(columns=rename)
        if '貨號' not in df.columns or '品名' not in df.columns: return None, "缺貨號或品名"
        for c in ['系列','分類','規格']: 
            if c not in df.columns: df[c] = '未分類'
        return df[['貨號','品名','系列','分類','規格']].astype(str), "OK"
    except Exception as e: return None, str(e)

def process_opening(file, wh):
    try:
        df = pd.read_csv(file) if file.name.endswith('.csv') else pd.read_excel(file)
        rename = {'名稱':'品名','SKU':'貨號','庫存':'數量','成本':'進貨總成本'}
        df = df.rename(columns=rename)
        if '貨號' not in df.columns or '數量' not in df.columns: return None, "缺貨號或數量"
        
        recs = []
        inv = st.session_state['inventory']
        for _, row in df.iterrows():
            sku = str(row['貨號'])
            qty = safe_float(row['數量'])
            if qty <= 0: continue
            
            exist = inv[inv['貨號']==sku]
            if not exist.empty:
                ser, cat, name = exist.iloc[0]['系列'], exist.iloc[0]['分類'], exist.iloc[0]['品名']
                spec = exist.iloc[0]['規格']
            else:
                ser = row.get('系列','期初')
                cat = row.get('分類','期初')
                name = row.get('品名', f'未命名-{sku}')
                spec = row.get('規格','')
                
            recs.append({
                '單據類型':'期初建檔', '單號':f"OPEN-{int(time.time())}-{sku}",
                '日期':date.today(), '系列':ser, '分類':cat, '品名':name, '貨號':sku, '規格':spec,
                '批號':f"INIT-{date.today():%Y%m%d}", '倉庫':wh, '數量':qty,
                'Key單者':'匯入', '進貨總成本': safe_float(row.get('進貨總成本',0)), '備註':'期初匯入'
            })
        res_df = pd.DataFrame(recs)
        for c in HISTORY_COLUMNS:
            if c not in res_df.columns: res_df[c] = ""
        return res_df, "OK"
    except Exception as e: return None, str(e)

def process_restore(file):
    try:
        df = pd.read_excel(file, sheet_name='完整流水帳')
        for c in HISTORY_COLUMNS:
            if c not in df.columns: df[c] = ""
        df['數量'] = pd.to_numeric(df['數量'], errors='coerce').fillna(0)
        df['進貨總成本'] = pd.to_numeric(df['進貨總成本'], errors='coerce').fillna(0)
        return df
    except Exception as e: return None
    
# ==========================================
# 3. 初始化
# ==========================================

if 'inventory' not in st.session_state:
    inv, hist = load_data()
    st.session_state['inventory'] = inv
    st.session_state['history'] = hist

if 'sku_rules' not in st.session_state:
    st.session_state['sku_rules'] = load_rules()

# ==========================================
# 4. 主程式介面
# ==========================================

st.set_page_config(page_title=PAGE_TITLE, layout="wide", page_icon="🏭")
st.title(f"🏭 {PAGE_TITLE}")

with st.sidebar:
    st.header("部門功能導航")
    page = st.radio("前往", [
        "📦 商品建檔與維護", "⚖️ 庫存盤點與調整", 
        "📥 進貨庫存", "🔨 製造生產", "🚚 銷售出貨", 
        "📊 總表監控 (主管)", "💰 成本財務 (加密)"
    ])
    
    st.divider()
    st.caption("🔧 系統工具")
    
    if st.button("🔴 重置系統 (若遇錯誤請按此)"):
        st.session_state.clear()
        st.cache_data.clear()
        st.rerun()

    if not st.session_state['history'].empty:
        with st.expander("📥 下載報表"):
            st.download_button("📊 庫存現況.xlsx", convert_single_sheet_to_excel(st.session_state['inventory'], "庫存"), f"Stock_{date.today()}.xlsx")
            st.download_button("📥 進貨紀錄.xlsx", convert_single_sheet_to_excel(st.session_state['history'][st.session_state['history']['單據類型']=='進貨'], "進貨"), f"In_{date.today()}.xlsx")
            st.download_button("🚚 銷貨紀錄.xlsx", convert_single_sheet_to_excel(st.session_state['history'][st.session_state['history']['單據類型'].isin(['銷售出貨'])], "銷貨"), f"Out_{date.today()}.xlsx")
            st.download_button("🔨 製造紀錄.xlsx", convert_single_sheet_to_excel(st.session_state['history'][st.session_state['history']['單據類型'].str.contains('製造', na=False)], "製造"), f"Mfg_{date.today()}.xlsx")
            st.download_button("📜 完整備份.xlsx", convert_to_excel_all_sheets(st.session_state['inventory'], st.session_state['history']), f"Backup_{date.today()}.xlsx")

    with st.expander("⚙️ 上傳備份還原"):
        up_res = st.file_uploader("選備份檔", type=['xlsx'])
        if up_res and st.button("確認還原"):
            df = process_restore(up_res)
            if df is not None:
                st.session_state['history'] = df
                st.session_state['inventory'] = recalculate_inventory(df, st.session_state['inventory'])
                save_data()
                st.success("還原成功")
                time.sleep(1)
                st.rerun()

# ---------------------------------------------------------
# 頁面內容
# ---------------------------------------------------------

if page == "📦 商品建檔與維護":
    st.subheader("📦 商品資料庫")
    t1, t2, t3, t4, t5 = st.tabs(["✨ 建檔", "📂 匯入商品", "📥 匯入庫存", "⚙️ 編碼規則設定", "📋 檢視/修改"])
    
    with t4:
        st.info("請上傳包含 4 個分頁 (`類別規則`, `系列規則`, `品名規則`, `規格規則`) 的 Excel 檔。")
        c1, c2 = st.columns([1, 2])
        with c1:
            up_rule = st.file_uploader("上傳規則 Excel", type=['xlsx'], key='rule_up')
            if up_rule and st.button("更新規則"):
                new_rules, msg = process_rules_upload_v2(up_rule)
                if new_rules is not None:
                    st.session_state['sku_rules'] = new_rules
                    save_rules_to_excel(new_rules) 
                    st.success(f"規則更新成功：{msg}")
                    time.sleep(1); st.rerun()
                else:
                    st.error(msg)
        
        with c2:
            if st.button("🔴 清除所有規則"):
                empty_rules = {
                    'category': pd.DataFrame(columns=['名稱', '代碼']),
                    'series': pd.DataFrame(columns=['名稱', '代碼']),
                    'name': pd.DataFrame(columns=['名稱', '代碼']),
                    'spec': pd.DataFrame(columns=['名稱', '代碼'])
                }
                st.session_state['sku_rules'] = empty_rules
                if os.path.exists(RULES_FILE): os.remove(RULES_FILE)
                st.success("規則已清除")
                time.sleep(1); st.rerun()

            st.caption("目前生效的規則預覽：")
            rt_series, rt_cat, rt_name, rt_spec = st.tabs(["系列", "類別", "品名", "規格"])
            
            def show_rule_editor(rule_key, label):
                current_df = st.session_state['sku_rules'].get(rule_key, pd.DataFrame(columns=['名稱', '代碼']))
                edited = st.data_editor(current_df, num_rows="dynamic", key=f"edit_{rule_key}", use_container_width=True)
                if st.button(f"💾 儲存【{label}】變更", key=f"save_{rule_key}"):
                    st.session_state['sku_rules'][rule_key] = edited
                    save_rules_to_excel(st.session_state['sku_rules'])
                    st.success(f"{label} 已更新！")

            with rt_series: show_rule_editor('series', '系列規則')
            with rt_cat: show_rule_editor('category', '類別規則')
            with rt_name: show_rule_editor('name', '品名規則')
            with rt_spec: show_rule_editor('spec', '規格規則')

    with t1:
        c1, c2 = st.columns(2)
        # 僅顯示規則表中的選項，不顯示庫存舊資料
        ser_opts = get_dynamic_options('系列', DEFAULT_SERIES)
        ser = c1.selectbox("系列", ser_opts)
        ser = st.text_input("輸入新系列") if ser == "➕ 手動輸入新資料" else ser
        
        cat_opts = get_dynamic_options('分類', DEFAULT_CATEGORIES)
        cat = c2.selectbox("分類", cat_opts)
        cat = st.text_input("輸入新分類") if cat == "➕ 手動輸入新資料" else cat
        
        c3, c4 = st.columns(2)
        name = c3.text_input("品名")
        spec = c4.text_input("規格/尺寸")
        
        auto_sku = auto_generate_composite_sku(cat, ser, name, spec)
        sku = st.text_input("貨號 (自動組合)", value=auto_sku)
        
        if st.button("建立商品", type="primary"):
            if not name: st.error("缺品名")
            else:
                row = {'貨號':sku, '系列':ser, '分類':cat, '品名':name, '規格':spec, '總庫存':0, '均價':0}
                for w in WAREHOUSES: row[f'庫存_{w}'] = 0
                st.session_state['inventory'] = pd.concat([st.session_state['inventory'], pd.DataFrame([row])], ignore_index=True)
                save_data()
                st.success(f"建立成功: {name} ({sku})")
                time.sleep(1); st.rerun()

    with t2:
        up = st.file_uploader("上傳商品清單 (Excel)", key="p_up")
        if up and st.button("匯入商品"):
            df, msg = process_product_upload(up)
            if df is not None:
                old = st.session_state['inventory']
                for _, r in df.iterrows():
                    if r['貨號'] not in old['貨號'].values:
                        r['總庫存']=0; r['均價']=0
                        for w in WAREHOUSES: r[f'庫存_{w}']=0
                        old = pd.concat([old, pd.DataFrame([r.to_dict()])], ignore_index=True)
                st.session_state['inventory'] = sort_inventory(old)
                save_data(); st.success("匯入完成"); time.sleep(1); st.rerun()
            else: st.error(msg)
            
    with t3:
        target_wh = st.selectbox("預設倉庫", WAREHOUSES)
        up = st.file_uploader("上傳庫存盤點 (Excel)", key="s_up")
        if up and st.button("匯入庫存"):
            df, msg = process_opening(up, target_wh)
            if df is not None:
                st.session_state['history'] = pd.concat([st.session_state['history'], df], ignore_index=True)
                st.session_state['inventory'] = recalculate_inventory(st.session_state['history'], st.session_state['inventory'])
                save_data(); st.success("庫存已更新"); time.sleep(1); st.rerun()
            else: st.error(msg)

    with t5:
        df = get_safe_view(st.session_state['inventory'])
        edited = st.data_editor(filter_dataframe(df), num_rows="dynamic", use_container_width=True, key="inv_editor")
        if st.button("儲存修改"):
            curr = st.session_state['inventory']
            for idx, row in edited.iterrows():
                if idx in curr.index:
                    for col in ['品名','分類','系列','規格']: curr.at[idx, col] = row[col]
            st.session_state['inventory'] = sort_inventory(curr)
            save_data(); st.success("已更新")

elif page == "⚖️ 庫存盤點與調整":
    st.subheader("⚖️ 庫存調整")
    inv = st.session_state['inventory']
    if not inv.empty:
        inv = sort_inventory(inv)
        inv['label'] = inv['貨號'] + " | " + inv['品名']
        
        c1, c2 = st.columns([2,1])
        sel = c1.selectbox("商品", inv['label'])
        wh = c2.selectbox("倉庫", WAREHOUSES)
        
        sku = inv[inv['label']==sel].iloc[0]['貨號']
        curr = safe_float(inv[inv['貨號']==sku].iloc[0][f'庫存_{wh}'])
        
        st.metric("目前系統庫存", f"{int(curr)}")
        
        with st.form("adj"):
            val = int(curr) if curr >= 0 else 0
            new = st.number_input("實際盤點數量", min_value=0, value=val)
            reason = st.text_input("原因", "盤點修正")
            
            if st.form_submit_button("確認修正"):
                diff = new - curr
                if diff != 0:
                    act = "庫存調整(加)" if diff > 0 else "庫存調整(減)"
                    row = inv[inv['貨號']==sku].iloc[0]
                    rec = {
                        '單據類型':act, '單號':f"ADJ-{int(time.time())}", '日期':date.today(),
                        '系列':row['系列'], '分類':row['分類'], '品名':row['品名'], '貨號':sku, '規格':row['規格'],
                        '倉庫':wh, '數量':abs(diff), 'Key單者':'盤點', '備註':reason
                    }
                    for c in HISTORY_COLUMNS: 
                        if c not in rec: rec[c]=""
                    st.session_state['history'] = pd.concat([st.session_state['history'], pd.DataFrame([rec])], ignore_index=True)
                    st.session_state['inventory'] = recalculate_inventory(st.session_state['history'], st.session_state['inventory'])
                    save_data(); st.success("修正完成"); time.sleep(1); st.rerun()

elif page == "📥 進貨庫存 (無金額)":
    st.subheader("📥 進貨")
    inv = st.session_state['inventory']
    if not inv.empty:
        inv['label'] = inv['貨號'] + " | " + inv['品名']
        with st.expander("新增進貨", expanded=True):
            c1, c2 = st.columns([2,1])
            sel = c1.selectbox("商品", inv['label'])
            wh = c2.selectbox("倉庫", WAREHOUSES, index=0)
            
            c3, c4, c5 = st.columns(3)
            qty = c3.number_input("數量", 1)
            dt = c4.date_input("日期", date.today())
            user = c5.selectbox("經手人", DEFAULT_KEYERS)
            
            c6, c7 = st.columns(2)
            sup = c6.text_input("廠商")
            note = c7.text_input("備註")
            
            if st.button("確認進貨"):
                sku = inv[inv['label']==sel].iloc[0]['貨號']
                row = inv[inv['貨號']==sku].iloc[0]
                rec = {
                    '單據類型':'進貨', '單號':f"IN-{int(time.time())}", '日期':dt,
                    '系列':row['系列'], '分類':row['分類'], '品名':row['品名'], '貨號':sku, '規格':row['規格'],
                    '批號':gen_batch_number("IN"), '倉庫':wh, '數量':qty, 'Key單者':user,
                    '廠商':sup, '備註':note, '進貨總成本':0
                }
                st.session_state['history'] = pd.concat([st.session_state['history'], pd.DataFrame([rec])], ignore_index=True)
                st.session_state['inventory'] = recalculate_inventory(st.session_state['history'], st.session_state['inventory'])
                save_data(); st.success("進貨完成"); time.sleep(1); st.rerun()

        df = st.session_state['history']
        if not df.empty:
            view = df[df['單據類型']=='進貨'].copy()
            cols = ['單號','日期','廠商','品名','規格','數量','倉庫','Key單者','備註']
            view = view[[c for c in cols if c in view.columns]]
            st.dataframe(filter_dataframe(view), use_container_width=True)

elif page == "🚚 銷售出貨 (業務/出貨)":
    st.subheader("🚚 出貨")
    inv = st.session_state['inventory']
    if not inv.empty:
        inv['label'] = inv['貨號'] + " | " + inv['品名']
        with st.expander("新增出貨", expanded=True):
            c1, c2 = st.columns([2,1])
            sel = c1.selectbox("商品", inv['label'])
            wh = c2.selectbox("倉庫", WAREHOUSES, index=2)
            
            c3, c4, c5 = st.columns(3)
            qty = c3.number_input("數量", 1)
            fee = c4.number_input("運費", 0)
            dt = c5.date_input("日期", date.today())
            
            c6, c7 = st.columns(2)
            ord_no = c6.text_input("訂單號")
            user = c7.selectbox("經手人", DEFAULT_KEYERS)
            
            if st.button("確認出貨"):
                sku = inv[inv['label']==sel].iloc[0]['貨號']
                row = inv[inv['貨號']==sku].iloc[0]
                rec = {
                    '單據類型':'銷售出貨', '單號':f"OUT-{int(time.time())}", '日期':dt,
                    '系列':row['系列'], '分類':row['分類'], '品名':row['品名'], '貨號':sku, '規格':row['規格'],
                    '倉庫':wh, '數量':qty, 'Key單者':user, '訂單單號':ord_no, '運費':fee
                }
                st.session_state['history'] = pd.concat([st.session_state['history'], pd.DataFrame([rec])], ignore_index=True)
                st.session_state['inventory'] = recalculate_inventory(st.session_state['history'], st.session_state['inventory'])
                save_data(); st.success("出貨完成"); time.sleep(1); st.rerun()
                
        df = st.session_state['history']
        if not df.empty:
            view = df[df['單據類型'].isin(['銷售出貨'])].copy()
            cols = ['單號','訂單單號','日期','品名','規格','數量','運費','Key單者']
            view = view[[c for c in cols if c in view.columns]]
            st.dataframe(filter_dataframe(view), use_container_width=True)

elif page == "🔨 製造生產 (工廠)":
    st.subheader("🔨 製造")
    inv = st.session_state['inventory']
    if not inv.empty:
        inv['label'] = inv['貨號'] + " | " + inv['品名']
        t1, t2 = st.tabs(["領料", "完工"])
        with t1:
            c1, c2 = st.columns(2)
            sel = c1.selectbox("原料", inv['label'])
            wh = c2.selectbox("從哪領", WAREHOUSES, index=0)
            qty = st.number_input("量", 1)
            if st.button("領料"):
                sku = inv[inv['label']==sel].iloc[0]['貨號']
                row = inv[inv['貨號']==sku].iloc[0]
                rec = {
                    '單據類型':'製造領料', '單號':f"MO-{int(time.time())}", '日期':date.today(),
                    '系列':row['系列'], '分類':row['分類'], '品名':row['品名'], '貨號':sku, '規格':row['規格'],
                    '倉庫':wh, '數量':qty, 'Key單者':'工廠'
                }
                st.session_state['history'] = pd.concat([st.session_state['history'], pd.DataFrame([rec])], ignore_index=True)
                st.session_state['inventory'] = recalculate_inventory(st.session_state['history'], st.session_state['inventory'])
                save_data(); st.success("已領料"); time.sleep(1); st.rerun()

        with t2:
            c1, c2 = st.columns(2)
            sel = c1.selectbox("成品", inv['label'])
            wh = c2.selectbox("入庫至", WAREHOUSES, index=1)
            qty = st.number_input("產出量", 1)
            if st.button("完工"):
                sku = inv[inv['label']==sel].iloc[0]['貨號']
                row = inv[inv['貨號']==sku].iloc[0]
                rec = {
                    '單據類型':'製造入庫', '單號':f"PD-{int(time.time())}", '日期':date.today(),
                    '系列':row['系列'], '分類':row['分類'], '品名':row['品名'], '貨號':sku, '規格':row['規格'],
                    '倉庫':wh, '數量':qty, 'Key單者':'工廠', '批號':gen_batch_number("PD")
                }
                st.session_state['history'] = pd.concat([st.session_state['history'], pd.DataFrame([rec])], ignore_index=True)
                st.session_state['inventory'] = recalculate_inventory(st.session_state['history'], st.session_state['inventory'])
                save_data(); st.success("完工入庫"); time.sleep(1); st.rerun()

elif page == "📊 總表監控 (主管)":
    st.subheader("📊 總表監控")
    pwd = st.text_input("密碼", type="password")
    if pwd == ADMIN_PASSWORD:
        t1, t2 = st.tabs(["庫存", "流水帳"])
        with t1:
            edited = st.data_editor(filter_dataframe(st.session_state['inventory']), num_rows="dynamic")
            if st.button("存庫存"): 
                st.session_state['inventory'] = sort_inventory(edited)
                save_data(); st.success("已存")
        with t2:
            edited = st.data_editor(filter_dataframe(st.session_state['history']), num_rows="dynamic")
            if st.button("存流水帳"):
                st.session_state['history'] = edited
                st.session_state['inventory'] = recalculate_inventory(edited, st.session_state['inventory'])
                save_data(); st.success("已存")

elif page == "💰 成本與財務管理 (加密)":
    st.subheader("💰 財務")
    pwd = st.text_input("密碼", type="password")
    if pwd == ADMIN_PASSWORD:
        df = st.session_state['history']
        mask = (df['單據類型']=='進貨') & (df['進貨總成本']==0)
        edited = st.data_editor(filter_dataframe(df[mask]), key="cost_edit")
        if st.button("存成本"):
            df.update(edited)
            st.session_state['history'] = df
            st.session_state['inventory'] = recalculate_inventory(df, st.session_state['inventory'])
            save_data(); st.success("已存")
