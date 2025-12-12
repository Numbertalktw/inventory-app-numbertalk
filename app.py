import streamlit as st
import pandas as pd
from datetime import date, datetime
import os
import time

# ==========================================
# 1. 核心邏輯與設定區
# ==========================================

# 系統標準欄位 (已移除 '系列' 與 '尺寸規格')
COLUMNS = [
    '編號', '分類', '名稱', 
    '寬度mm', '長度mm', '形狀', '五行', 
    '進貨總價', '進貨數量(顆)', '進貨日期', '進貨廠商', 
    '庫存(顆)', '單顆成本'
]

# 歷史紀錄欄位
HISTORY_COLUMNS = [
    '紀錄時間', '單號', '動作', '編號', '分類', '名稱', '規格', 
    '廠商', '進貨數量', '進貨總價', '單價'
]

# 設計銷售紀錄欄位
DESIGN_HISTORY_COLUMNS = [
    '單號', '日期', '總顆數', '材料成本', '工資', '雜支', 
    '總成本', '售價(x3)', '售價(x5)', '明細內容'
]

DEFAULT_CSV_FILE = 'inventory_backup_v2.csv'
DESIGN_HISTORY_FILE = 'design_sales_history.csv'

# 預設選單資料
DEFAULT_SUPPLIERS = ["小聰頭", "廠商A", "廠商B", "自用", "蝦皮", "淘寶"]
DEFAULT_SHAPES = ["圓珠", "切角", "鑽切", "圓筒", "方體", "長柱", "不規則", "造型"]
DEFAULT_ELEMENTS = ["金", "木", "水", "火", "土", "綜合"]

# 初始範例資料
INITIAL_DATA = {
    '編號': ['ST0001'],
    '分類': ['天然石'],
    '名稱': ['範例水晶'],
    '寬度mm': [6.0],
    '長度mm': [6.0],
    '形狀': ['圓珠'],
    '五行': ['綜合'],
    '進貨總價': [100],
    '進貨數量(顆)': [100],
    '進貨日期': [str(date.today())],
    '進貨廠商': ['範例廠商'],
    '庫存(顆)': [100],
    '單顆成本': [1.0]
}

# ==========================================
# 2. 核心邏輯函式
# ==========================================

def save_inventory():
    """儲存庫存"""
    try:
        if 'inventory' in st.session_state:
            st.session_state['inventory'].to_csv(DEFAULT_CSV_FILE, index=False, encoding='utf-8-sig')
    except Exception: pass

def save_design_history():
    """儲存銷售紀錄"""
    try:
        if 'design_history' in st.session_state:
            st.session_state['design_history'].to_csv(DESIGN_HISTORY_FILE, index=False, encoding='utf-8-sig')
    except Exception: pass

def normalize_columns(df):
    """標準化欄位名稱並移除舊的多餘欄位"""
    rename_map = {
        '尺寸': '寬度mm', 'Size': '寬度mm', '寬度': '寬度mm', 'Width': '寬度mm',
        '長度': '長度mm', 'Length': '長度mm',
        'Name': '名稱', 'Category': '分類', 'Code': '編號', 'ID': '編號',
        '尺寸規格': '備註規格' # 將舊的尺寸規格暫存，避免遺失，但主要邏輯改用寬/長
    }
    df = df.rename(columns=rename_map)
    
    # 補齊欄位
    for col in COLUMNS:
        if col not in df.columns:
            if 'mm' in col or '價' in col or '數量' in col or '成本' in col:
                df[col] = 0
            else:
                df[col] = ""
    
    # 只保留 COLUMNS 定義的欄位
    return df[COLUMNS]

def generate_new_id(category, df):
    """產生 ST/AC/OT 編號"""
    prefix_map = {'天然石': 'ST', '配件': 'AC', '耗材': 'OT'}
    prefix = prefix_map.get(category, "OT")
    
    if df.empty or '編號' not in df.columns: return f"{prefix}0001"
    
    df_ids = df['編號'].astype(str)
    mask = df_ids.str.startswith(prefix, na=False)
    numeric_part = df_ids[mask].str[2:].str.extract(r'(\d+)', expand=False).dropna()
    
    if numeric_part.empty: next_num = 1
    else: next_num = numeric_part.astype(int).max() + 1
    
    return f"{prefix}{next_num:04d}"

def merge_inventory_duplicates(df):
    """合併重複項目"""
    if df.empty: return df, 0
    group_cols = ['分類', '名稱', '寬度mm', '長度mm', '形狀', '五行']
    
    if not set(group_cols).issubset(df.columns): return df, 0
    
    work_df = df.copy()
    work_df['庫存(顆)'] = pd.to_numeric(work_df['庫存(顆)'], errors='coerce').fillna(0)
    work_df['單顆成本'] = pd.to_numeric(work_df['單顆成本'], errors='coerce').fillna(0)
    original_count = len(work_df)
    
    work_df['總成本'] = work_df['庫存(顆)'] * work_df['單顆成本']
    
    agg = work_df.groupby(group_cols, as_index=False).agg({
        '庫存(顆)': 'sum', '總成本': 'sum', '進貨日期': 'max'
    })
    agg['單顆成本'] = agg.apply(lambda r: (r['總成本'] / r['庫存(顆)']) if r['庫存(顆)'] > 0 else 0, axis=1)
    agg = agg.drop(columns=['總成本'])
    
    work_df['進貨日期_排序'] = pd.to_datetime(work_df['進貨日期'], errors='coerce')
    base_rows = work_df.sort_values(['進貨日期_排序', '編號'], ascending=[False, False]).groupby(group_cols, as_index=False).first()
    
    final_df = pd.merge(agg, base_rows[['編號', '進貨廠商'] + group_cols], on=group_cols, how='left')
    
    return normalize_columns(final_df), original_count - len(final_df)

def format_size(row):
    """將寬度與長度格式化為易讀字串"""
    try:
        w = float(row.get('寬度mm', 0))
        l = float(row.get('長度mm', 0))
        if w > 0:
            if l == 0 or l == w: return f"{w}mm"
            else: return f"{w}x{l}mm"
    except: pass
    return ""

def make_inventory_label(row):
    size_str = format_size(row)
    size_disp = f"({size_str})" if size_str else ""
    return f"{str(row['編號'])} | {str(row['名稱'])} {size_disp} | 存:{row['庫存(顆)']}"

def make_design_label(row):
    size_str = format_size(row)
    size_disp = f"({size_str})" if size_str else ""
    shape_str = str(row.get('形狀', '')).strip()
    return f"【{str(row['五行'])}】{str(row['名稱'])} | {shape_str} {size_disp} | ${float(row['單顆成本']):.1f}/顆 | 存:{row['庫存(顆)']}"

def get_dynamic_options(column_name, default_list):
    options = set(default_list)
    if not st.session_state['inventory'].empty:
        existing = st.session_state['inventory'][column_name].dropna().unique().tolist()
        options.update([str(x) for x in existing if str(x).strip() != ""])
    return ["➕ 手動輸入/新增"] + sorted(list(options))

# ==========================================
# 3. 初始化 Session State
# ==========================================

if 'inventory' not in st.session_state:
    if os.path.exists(DEFAULT_CSV_FILE):
        try:
            df = pd.read_csv(DEFAULT_CSV_FILE)
            st.session_state['inventory'] = normalize_columns(df)
        except: st.session_state['inventory'] = pd.DataFrame(columns=COLUMNS)
    else: st.session_state['inventory'] = pd.DataFrame(columns=COLUMNS)

# 強制正規化與修復
if 'inventory' in st.session_state:
    st.session_state['inventory'] = normalize_columns(st.session_state['inventory'])

if 'history' not in st.session_state:
    st.session_state['history'] = pd.DataFrame(columns=HISTORY_COLUMNS)
else:
    if '單號' not in st.session_state['history'].columns:
        st.session_state['history'].insert(1, '單號', '')

if 'design_history' not in st.session_state:
    if os.path.exists(DESIGN_HISTORY_FILE):
        try:
            st.session_state['design_history'] = pd.read_csv(DESIGN_HISTORY_FILE)
        except: st.session_state['design_history'] = pd.DataFrame(columns=DESIGN_HISTORY_COLUMNS)
    else: st.session_state['design_history'] = pd.DataFrame(columns=DESIGN_HISTORY_COLUMNS)

if 'current_design' not in st.session_state:
    st.session_state['current_design'] = []

# ==========================================
# 4. UI 介面
# ==========================================

st.set_page_config(page_title="GemCraft 庫存管理系統", layout="wide")
st.title("💎 GemCraft 庫存管理系統")

with st.sidebar:
    st.header("功能導航")
    page = st.radio("前往", ["📦 庫存管理與進貨", "📜 進貨紀錄查詢", "🧮 設計與成本計算"])
    st.divider()
    
    if not st.session_state['inventory'].empty:
        csv = st.session_state['inventory'].to_csv(index=False).encode('utf-8-sig')
        st.download_button("📥 下載庫存 (CSV)", csv, f'inventory_{date.today()}.csv', "text/csv")
        
    uploaded_inv = st.file_uploader("📤 上傳庫存備份 (CSV)", type=['csv'])
    if uploaded_inv:
        try:
            df = pd.read_csv(uploaded_inv)
            st.session_state['inventory'] = normalize_columns(df)
            save_inventory()
            st.success("庫存還原成功！")
            st.rerun()
        except Exception as e: st.error(f"讀取失敗: {e}")

# ------------------------------------------
# 頁面: 庫存管理 (★ 這裡修正了 SyntaxError ★)
# ------------------------------------------
if page == "📦 庫存管理與進貨":
    st.subheader("📦 庫存管理")
    tab1, tab2, tab3 = st.tabs(["🔄 舊品補貨", "✨ 建立新商品", "🛠️ 修改與刪除"])
    
    # === Tab 1: 補貨 ===
    with tab1:
        st.caption("已有編號商品補貨")
        inv_df = st.session_state['inventory']
        if not inv_df.empty:
            inv_df['label'] = inv_df.apply(make_inventory_label, axis=1)
            target_label = st.selectbox("選擇商品", inv_df['label'].tolist())
            target_row = inv_df[inv_df['label'] == target_label].iloc[0]
            target_idx = inv_df[inv_df['label'] == target_label].index[0]
            
            with st.form("restock"):
                st.write(f"目前庫存: **{target_row['庫存(顆)']}**")
                batch_no = st.text_input("進貨單號 (選填)", placeholder="Auto")
                c1, c2 = st.columns(2)
                qty = c1.number_input("進貨數量", 1)
                cost = c2.number_input("進貨總價", 0)
                
                if st.form_submit_button("📦 確認補貨"):
                    new_qty = target_row['庫存(顆)'] + qty
                    old_val = target_row['庫存(顆)'] * target_row['單顆成本']
                    new_avg = (old_val + cost) / new_qty if new_qty > 0 else 0
                    
                    st.session_state['inventory'].at[target_idx, '庫存(顆)'] = new_qty
                    st.session_state['inventory'].at[target_idx, '單顆成本'] = new_avg
                    st.session_state['inventory'].at[target_idx, '進貨日期'] = date.today()
                    
                    log = {
                        '紀錄時間': datetime.now().strftime("%Y-%m-%d %H:%M"),
                        '單號': batch_no if batch_no else f"AUTO-{int(time.time())}",
                        '動作': '補貨',
                        '編號': target_row['編號'], '分類': target_row['分類'], '名稱': target_row['名稱'],
                        '規格': format_size(target_row), '廠商': target_row['進貨廠商'],
                        '進貨數量': qty, '進貨總價': cost, '單價': cost/qty if qty>0 else 0
                    }
                    st.session_state['history'] = pd.concat([st.session_state['history'], pd.DataFrame([log])], ignore_index=True)
                    save_inventory()
                    st.success("補貨成功！")
                    st.rerun()
        else: st.info("無庫存")

    # === Tab 2: 建立新商品 ===
    with tab2:
        with st.container():
            st.markdown("##### 1. 基本資料")
            c1, c2 = st.columns([1, 2])
            with c1: 
                new_cat = st.selectbox("分類 (產生編號用)", ["天然石", "配件", "耗材"])
            with c2:
                existing_names = []
                if not st.session_state['inventory'].empty:
                    cat_df = st.session_state['inventory'][st.session_state['inventory']['分類'] == new_cat]
                    existing_names = sorted(cat_df['名稱'].dropna().unique().astype(str).tolist())
                name_opts = ["➕ 手動輸入/新增"] + existing_names
                name_sel = st.selectbox("名稱", name_opts)
                final_name = st.text_input("↳ 輸入新名稱") if name_sel == "➕ 手動輸入/新增" else name_sel

            st.markdown("##### 2. 規格尺寸 (mm)")
            c3a, c3b = st.columns(2)
            with c3a:
                w_opts = get_dynamic_options('寬度mm', [])
                w_sel = st.selectbox("寬度/直徑", w_opts)
                final_w = st.number_input("↳ 輸入寬度", 0.0, step=0.5) if w_sel == "➕ 手動輸入/新增" else float(w_sel)
            with c3b:
                final_l = st.number_input("長度 (圓珠可不填)", 0.0, step=0.5)
                if final_l == 0.0 and final_w > 0: st.caption(f"預設為 {final_w}")

        prev_row = None
        if final_name and not st.session_state['inventory'].empty:
            same_name_df = st.session_state['inventory'][(st.session_state['inventory']['分類'] == new_cat) & (st.session_state['inventory']['名稱'] == final_name)]
            if not same_name_df.empty:
                prev_row = same_name_df.iloc[-1]

        with st.form("add_new"):
            st.markdown("##### 3. 詳細資訊")
            batch_id_new = st.text_input("進貨單號 (選填)", placeholder="Auto")

            shape_opts = get_dynamic_options('形狀', DEFAULT_SHAPES)
            elem_opts = get_dynamic_options('五行', DEFAULT_ELEMENTS)
            sup_opts = get_dynamic_options('進貨廠商', DEFAULT_SUPPLIERS)
            
            def get_idx(opts, val):
                try: return opts.index(val)
                except: return 0

            idx_s = get_idx(shape_opts, prev_row['形狀']) if prev_row is not None else 0
            idx_e = get_idx(elem_opts, prev_row['五行']) if prev_row is not None else 0
            idx_p = get_idx(sup_opts, prev_row['進貨廠商']) if prev_row is not None else 0

            c4, c5, c6 = st.columns(3)
            with c4: s_sel = st.selectbox("形狀", shape_opts, index=idx_s)
            with c5: e_sel = st.selectbox("五行", elem_opts, index=idx_e)
            with c6: p_sel = st.selectbox("廠商", sup_opts, index=idx_p)
            
            mc1, mc2, mc3 = st.columns(3)
            final_shape = mc1.text_input("↳ 新形狀") if s_sel == "➕ 手動輸入/新增" else s_sel
            final_elem = mc2.text_input("↳ 新五行") if e_sel == "➕ 手動輸入/新增" else e_sel
            final_sup = mc3.text_input("↳ 新廠商") if p_sel == "➕ 手動輸入/新增" else p_sel

            c7, c8, c9 = st.columns(3)
            with c7: price = st.number_input("進貨總價", 0)
            with c8: qty = st.number_input("進貨數量", 1)
            with c9: p_date = st.date_input("進貨日期", value=date.today())
            
            if st.form_submit_button("➕ 確認新增入庫", type="primary"):
                if not all([final_name, final_shape, final_elem, final_sup]):
                    st.error("❌ 請填寫完整欄位")
                else:
                    save_l = final_l if final_l > 0 else (final_w if "圓" in final_shape or "珠" in final_shape else 0.0)
                    new_id = generate_new_id(new_cat, st.session_state['inventory'])
                    unit_cost = price / qty if qty > 0 else 0
                    
                    new_row = {
                        '編號': new_id, '分類': new_cat, '名稱': final_name, 
                        '寬度mm': final_w, '長度mm': save_l,
                        '形狀': final_shape, '五行': final_elem, 
                        '進貨總價': price, '進貨數量(顆)': qty, 
                        '進貨日期': p_date, '進貨廠商': final_sup,
                        '庫存(顆)': qty, '單顆成本': unit_cost
                    }
                    st.session_state['inventory'] = pd.concat([st.session_state['inventory'], pd.DataFrame([new_row])], ignore_index=True)
                    
                    log = {
                        '紀錄時間': datetime.now().strftime("%Y-%m-%d %H:%M"), 
                        '單號': batch_id_new if batch_id_new else f"AUTO-{int(time.time())}", 
                        '動作': '進貨',
                        '編號': new_id, '分類': new_cat, '名稱': final_name,
                        '規格': f"{final_w}x{save_l}mm", '形狀': final_shape,
                        '廠商': final_sup, '進貨數量': qty, '進貨總價': price, '單價': unit_cost
                    }
                    st.session_state['history'] = pd.concat([st.session_state['history'], pd.DataFrame([log])], ignore_index=True)
                    save_inventory()
                    st.success(f"✅ 已新增：{final_name} - {new_id}")
                    time.sleep(1)
                    st.rerun()

    # === Tab 3: 修改 ===
    with tab3:
        st.markdown("##### 🛠️ 修正或刪除")
        if not st.session_state['inventory'].empty:
            edit_df = st.session_state['inventory'].copy()
            edit_df['label'] = edit_df.apply(make_inventory_label, axis=1)
            
            sel_label = st.selectbox("🔍 選擇要修改的商品", edit_df['label'].tolist())
            orig_row = edit_df[edit_df['label'] == sel_label].iloc[0]
            orig_idx = st.session_state['inventory'][st.session_state['inventory']['編號'] == orig_row['編號']].index[0]

            with st.form("edit_form"):
                st.info(f"編輯中：{orig_row['編號']}")
                ec1, ec2, ec3 = st.columns(3)
                with ec1: ename = st.text_input("名稱", value=orig_row['名稱'])
                with ec2: ewidth = st.number_input("寬度mm", value=float(orig_row['寬度mm']), step=0.1)
                with ec3: elength = st.number_input("長度mm", value=float(orig_row['長度mm']), step=0.1)

                shp_opts = get_dynamic_options('形狀', DEFAULT_SHAPES)
                elm_opts = get_dynamic_options('五行', DEFAULT_ELEMENTS)
                sup_opts = get_dynamic_options('進貨廠商', DEFAULT_SUPPLIERS)
                
                def get_eidx(opts, val):
                    try: return opts.index(val)
                    except: return 0

                ec4, ec5, ec6 = st.columns(3)
                with ec4: eshp_sel = st.selectbox("形狀", shp_opts, index=get_eidx(shp_opts, orig_row['形狀']))
                with ec5: eelm_sel = st.selectbox("五行", elm_opts, index=get_eidx(elm_opts, orig_row['五行']))
                with ec6: esup_sel = st.selectbox("廠商", sup_opts, index=get_eidx(sup_opts, orig_row['進貨廠商']))

                em1, em2, em3 = st.columns(3)
                eshape = em1.text_input("↳ 新形狀") if eshp_sel == "➕ 手動輸入/新增" else eshp_sel
                eelem = em2.text_input("↳ 新五行") if eelm_sel == "➕ 手動輸入/新增" else eelm_sel
                esup = em3.text_input("↳ 新廠商") if esup_sel == "➕ 手動輸入/新增" else esup_sel

                st.divider()
                ec7, ec8 = st.columns(2)
                with ec7: estock = st.number_input("庫存數量", value=int(orig_row['庫存(顆)']), step=1)
                with ec8: ecost = st.number_input("單顆成本", value=float(orig_row['單顆成本']), step=0.1, format="%.2f")

                bt1, bt2 = st.columns([1, 1])
                with bt1:
                    if st.form_submit_button("💾 儲存修改"):
                        st.session_state['inventory'].at[orig_idx, '名稱'] = ename
                        st.session_state['inventory'].at[orig_idx, '寬度mm'] = ewidth
                        st.session_state['inventory'].at[orig_idx, '長度mm'] = elength
                        st.session_state['inventory'].at[orig_idx, '形狀'] = eshape
                        st.session_state['inventory'].at[orig_idx, '五行'] = eelem
                        st.session_state['inventory'].at[orig_idx, '進貨廠商'] = esup
                        st.session_state['inventory'].at[orig_idx, '庫存(顆)'] = estock
                        st.session_state['inventory'].at[orig_idx, '單顆成本'] = ecost
                        save_inventory()
                        st.success("✅ 更新成功")
                        time.sleep(1)
                        st.rerun()

                with bt2:
                    if st.form_submit_button("🗑️ 刪除商品", type="primary"):
                        st.session_state['inventory'] = st.session_state['inventory'].drop(orig_idx).reset_index(drop=True)
                        save_inventory()
                        st.success("已刪除")
                        time.sleep(1)
                        st.rerun()
        else:
            st.info("無資料")

    st.divider()
    c_list1, c_list2 = st.columns([3, 1])
    with c_list1: st.markdown("### 📋 庫存總表")
    with c_list2:
        if st.button("🔄 合併重複項目"):
            mdf, cnt = merge_inventory_duplicates(st.session_state['inventory'])
            st.session_state['inventory'] = mdf
            save_inventory()
            st.success(f"已合併 {cnt} 筆")
            time.sleep(1)
            st.rerun()

    search = st.text_input("🔍 搜尋庫存", "")
    disp_df = st.session_state['inventory']
    if search:
        disp_df = disp_df[
            disp_df['名稱'].astype(str).str.contains(search, case=False) |
            disp_df['編號'].astype(str).str.contains(search, case=False)
        ]
    st.dataframe(disp_df, use_container_width=True, height=400,
                 column_config={
                     "進貨總價": st.column_config.NumberColumn(format="$%d"),
                     "單顆成本": st.column_config.NumberColumn(format="$%.2f"),
                     "寬度mm": st.column_config.NumberColumn(format="%.1f"),
                     "長度mm": st.column_config.NumberColumn(format="%.1f")
                 })

# ------------------------------------------
# 頁面 B: 紀錄
# ------------------------------------------
elif page == "📜 進貨紀錄查詢":
    st.subheader("📜 歷史紀錄中心")
    tab_log, tab_sales = st.tabs(["📦 庫存異動流水帳", "💎 訂單銷售紀錄"])
    with tab_log: st.dataframe(st.session_state['history'], use_container_width=True)
    with tab_sales: st.dataframe(st.session_state['design_history'], use_container_width=True)

# ------------------------------------------
# 頁面 C: 設計與成本
# ------------------------------------------
elif page == "🧮 設計與成本計算":
    st.subheader("🧮 成本試算與報價")
    
    inv = st.session_state['inventory']
    if not inv.empty:
        # 五行篩選
        all_elements = sorted(inv['五行'].astype(str).unique().tolist())
        st.write("👇 **第一步：選擇五行屬性 (可複選)**")
        selected_elements = st.multiselect("五行", all_elements, default=all_elements)
        if not selected_elements: selected_elements = all_elements
        
        filtered_inv = inv[inv['五行'].isin(selected_elements)].sort_values(by=['五行', '名稱', '編號'])

        st.divider()

        if not filtered_inv.empty:
            filtered_inv['disp_label'] = filtered_inv.apply(make_design_label, axis=1)
            
            c_sel, c_qty, c_btn = st.columns([3, 1, 1])
            with c_sel:
                selected_item_label = st.selectbox(
                    f"👇 選擇材料 (篩選後共 {len(filtered_inv)} 筆)", 
                    filtered_inv['disp_label'].tolist()
                )
            
            with c_qty:
                input_qty = st.number_input("數量", min_value=1, value=1)
            
            with c_btn:
                st.write("") 
                st.write("") 
                if st.button("⬇️ 加入清單", use_container_width=True, type="primary"):
                    selected_row = filtered_inv[filtered_inv['disp_label'] == selected_item_label].iloc[0]
                    subtotal = selected_row['單顆成本'] * input_qty
                    
                    st.session_state['current_design'].append({
                        '編號': selected_row['編號'], 
                        '名稱': selected_row['名稱'],
                        '形狀': selected_row['形狀'], 
                        '規格': format_size(selected_row), 
                        '單價': selected_row['單顆成本'], 
                        '數量': input_qty, 
                        '小計': subtotal
                    })
                    st.success(f"已加入 {input_qty} 顆 {selected_row['名稱']}")

            st.divider()
            
            if st.session_state['current_design']:
                df_design = pd.DataFrame(st.session_state['current_design'])
                
                # 防呆
                for col in ['名稱', '形狀', '規格', '單價', '數量', '小計']:
                    if col not in df_design.columns: df_design[col] = 0

                st.table(df_design[['名稱', '形狀', '規格', '單價', '數量', '小計']])
                
                if st.button("🗑️ 清除最後一項"):
                    st.session_state['current_design'].pop()
                    st.rerun()
                
                mat_cost = df_design['小計'].sum()
                st.markdown("#### 💰 成本結構")
                c_labor, c_misc = st.columns(2)
                labor = c_labor.number_input("工資 ($)", 0, step=10)
                misc = c_misc.number_input("雜支/運費 ($)", 0, step=5)
                
                total_base = mat_cost + labor + misc
                price_x3 = (mat_cost * 3) + labor + misc
                price_x5 = (mat_cost * 5) + labor + misc
                
                st.info(f"💎 基礎材料費: ${mat_cost:.1f}")
                m1, m2, m3 = st.columns(3)
                m1.metric("總成本", f"${total_base:.0f}")
                m2.metric("建議售價 (x3)", f"${price_x3:.0f}")
                m3.metric("建議售價 (x5)", f"${price_x5:.0f}")
                
                st.divider()
                sale_id = st.text_input("訂單編號", placeholder="例如: 蝦皮241212...")
                
                if st.button("✅ 確認售出 (扣除庫存並記錄)", type="primary"):
                    if not sale_id: sale_id = f"S-{int(time.time())}"
                    
                    detail_str = []
                    total_qty = 0
                    
                    for item in st.session_state['current_design']:
                        mask = st.session_state['inventory']['編號'] == item['編號']
                        if mask.any():
                            idx = st.session_state['inventory'][mask].index[0]
                            st.session_state['inventory'].at[idx, '庫存(顆)'] -= item['數量']
                        
                        log = {
                            '紀錄時間': datetime.now().strftime("%Y-%m-%d %H:%M"),
                            '單號': sale_id, '動作': '售出',
                            '編號': item['編號'], '分類': '', '名稱': item['名稱'], 
                            '規格': item['規格'], '進貨數量': -item['數量'],
                            '進貨總價': 0, '單價': item['單價']
                        }
                        st.session_state['history'] = pd.concat([st.session_state['history'], pd.DataFrame([log])], ignore_index=True)
                        detail_str.append(f"{item['名稱']}({item['編號']})x{item['數量']}")
                        total_qty += item['數量']
                    
                    design_log = {
                        '單號': sale_id, '日期': date.today(), '總顆數': total_qty,
                        '材料成本': mat_cost, '工資': labor, '雜支': misc,
                        '總成本': total_base, '售價(x3)': price_x3, '售價(x5)': price_x5,
                        '明細內容': " | ".join(detail_str)
                    }
                    st.session_state['design_history'] = pd.concat(
                        [st.session_state['design_history'], pd.DataFrame([design_log])], ignore_index=True
                    )
                    
                    save_inventory()
                    save_design_history()
                    st.session_state['current_design'] = []
                    st.success(f"已完成售出扣帳！單號：{sale_id}")
                    time.sleep(1)
                    st.rerun()
        else:
            st.warning("⚠️ 找不到符合條件的庫存項目，請檢查五行篩選或庫存是否為空。")
    else:
        st.info("目前庫存為空，請先至「📦 庫存管理」建立商品。")
