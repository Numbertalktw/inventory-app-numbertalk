import streamlit as st
import pandas as pd
from datetime import date, datetime
import os
import time

# ==========================================
# 1. 核心邏輯與設定區
# ==========================================

# 系統標準欄位順序
COLUMNS = [
    '編號', '分類', '名稱', '寬度mm', '長度mm', '形狀', '五行', 
    '進貨總價', '進貨數量(顆)', '進貨日期', '進貨廠商', '庫存(顆)', '單顆成本'
]

# 歷史紀錄欄位 (含單號)
HISTORY_COLUMNS = [
    '紀錄時間', '單號', '動作', '編號', '分類', '名稱', '寬度mm', '長度mm', '形狀', 
    '廠商', '進貨數量', '進貨總價', '單價'
]

DEFAULT_CSV_FILE = 'inventory_backup_v2.csv'

# 預設選單資料
DEFAULT_SUPPLIERS = [
    "小聰頭", "小聰頭-13", "小聰頭-千千", "小聰頭-子馨", "小聰頭-小宇", "小聰頭-尼克", "小聰頭-周三寶", "小聰頭-蒨",
    "永安", "石之靈", "多加市集", "決益X", "昇輝", "星辰Crystal", "珍珠包金", "格魯特", "御金坊",
    "TB-天使街", "TB-東吳天然石坊", "TB-物物居", "TB-軒閣珠寶", "TB-鈦鋼潮牌", "TB-義烏卡樂芙", 
    "TB-鼎喜", "TB-銀拍檔", "TB-廣州小銀子", "TB-慶和銀飾", "TB-賽維雅珠寶", "TB-ins網紅玻璃杯",
    "TB-Mary", "TB-Super Search",
    "祥玥", "雪霖", "晶格格", "愛你一生", "福祿壽銀飾", "億伙", "廠商", "寶城水晶", "Rich"
]
DEFAULT_SHAPES = ["圓珠", "切角", "鑽切", "圓筒", "方體", "長柱", "不規則", "造型"]
DEFAULT_ELEMENTS = ["金", "木", "水", "火", "土", "綜合"]

# 初始範例資料
INITIAL_DATA = {
    '編號': ['ST0001', 'ST0002', 'ST0003', 'ST0004', 'ST0005', 'ST0006'],
    '分類': ['天然石', '天然石', '天然石', '天然石', '天然石', '天然石'],
    '名稱': ['冰翠玉', '東菱玉', '紫水晶', '東菱玉', '東菱玉', '綠碧璽'],
    '寬度mm': [3.0, 5.0, 8.0, 6.0, 8.0, 8.0],
    '長度mm': [3.0, 5.0, 8.0, 6.0, 8.0, 8.0],
    '形狀': ['切角', '切角', '圓珠', '切角', '切角', '圓珠'],
    '五行': ['木', '木', '火', '木', '木', '木'],
    '進貨總價': [100, 180, 450, 132, 100, 550],
    '進貨數量(顆)': [145, 45, 50, 120, 45, 20],
    '進貨日期': ['2024-11-07', '2024-08-14', '2024-08-09', '2024-12-30', '2024-12-30', '2025-12-09'],
    '進貨廠商': ['TB-東吳天然石坊', 'Rich', '永安', 'TB-Super Search', 'TB-Super Search', '永安'],
    '庫存(顆)': [145, 45, 110, 120, 45, 20],
    '單顆成本': [0.689655, 4.0, 9.0, 1.1, 2.222222, 27.5],
}

# --- 工具函式 ---

def save_inventory_to_csv():
    try:
        if 'inventory' in st.session_state:
            st.session_state['inventory'].to_csv(DEFAULT_CSV_FILE, index=False, encoding='utf-8-sig')
    except Exception:
        pass

def generate_new_id(category, df):
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
    base_rows = base_rows.drop(columns=['庫存(顆)', '單顆成本', '總成本', '進貨日期_排序'], errors='ignore')
    merged = pd.merge(agg, base_rows, on=group_cols, how='left', suffixes=('', '_base'))
    if '進貨廠商_base' in merged.columns: merged['進貨廠商'] = merged['進貨廠商_base']
    if '編號_base' in merged.columns: merged['編號'] = merged['編號_base']
    merged = merged[[c for c in merged.columns if not c.endswith('_base')]]
    for col in COLUMNS:
        if col not in merged.columns:
            merged[col] = 0 if ('mm' in col or '數量' in col or '價' in col or '成本' in col) else ""
    return merged[COLUMNS], original_count - len(merged)

def normalize_columns(df):
    rename_map = {
        '尺寸': '寬度mm', '尺寸mm': '寬度mm', '尺寸(mm/cm)': '寬度mm', 'Size': '寬度mm', '寬度': '寬度mm', 'Width': '寬度mm',
        '長度': '長度mm', 'Length': '長度mm',
        '名称': '名稱', 'Name': '名稱',
        '分类': '分類', 'Category': '分類',
        '形状': '形狀', 'Shape': '形狀',
        '五行': '五行', 'Element': '五行',
        '库存(颗)': '庫存(顆)', 'Stock': '庫存(顆)', '库存': '庫存(顆)',
        '单颗成本': '單顆成本', 'Cost': '單顆成本', '成本': '單顆成本',
        '进货厂商': '進貨廠商', 'Supplier': '進貨廠商', '厂商': '進貨廠商',
        '进货日期': '進貨日期', 'Date': '進貨日期', '日期': '進貨日期',
        '进货總價': '進貨總價', 'Total Price': '進貨總價',
        '进货数量(颗)': '進貨數量(顆)', 'Qty': '進貨數量(顆)'
    }
    df = df.rename(columns=rename_map)
    if '長度mm' not in df.columns: df['長度mm'] = 0.0
    return df

def make_inventory_label(row):
    return f"{row['編號']} | {row['名稱']} ({row['寬度mm']}x{row['長度mm']}mm) | 存:{row['庫存(顆)']}"

def make_design_label(row):
    return f"【{row['五行']}】 {row['名稱']} | {row['形狀']} ({row['寬度mm']}x{row['長度mm']}mm) | ${row['單顆成本']:.1f}/顆 | 存:{row['庫存(顆)']}"

def get_dynamic_options(column_name, default_list):
    options = set(default_list)
    if not st.session_state['inventory'].empty:
        existing = st.session_state['inventory'][column_name].dropna().unique().tolist()
        options.update([str(x) for x in existing if str(x).strip() != ""])
    return ["➕ 手動輸入新資料"] + sorted(list(options))

# ==========================================
# 2. 初始化 Session State
# ==========================================

if 'inventory' not in st.session_state:
    file_loaded = False
    if os.path.exists(DEFAULT_CSV_FILE):
        try:
            df_init = pd.read_csv(DEFAULT_CSV_FILE)
            df_init = normalize_columns(df_init)
            df_init['編號'] = df_init['編號'].astype(str)
            df_init['單顆成本'] = pd.to_numeric(df_init['單顆成本'], errors='coerce').fillna(0)
            for col in COLUMNS:
                if col not in df_init.columns:
                    df_init[col] = 0 if ('mm' in col or '數量' in col or '價' in col or '成本' in col) else ""
            st.session_state['inventory'] = df_init[COLUMNS]
            file_loaded = True
        except Exception:
            pass
    if not file_loaded:
        st.session_state['inventory'] = pd.DataFrame(INITIAL_DATA)[COLUMNS]

if 'history' not in st.session_state:
    st.session_state['history'] = pd.DataFrame(columns=HISTORY_COLUMNS)
else:
    if '單號' not in st.session_state['history'].columns:
        st.session_state['history'].insert(1, '單號', '')

if 'current_design' not in st.session_state:
    st.session_state['current_design'] = []

# ==========================================
# 3. UI 介面
# ==========================================

st.set_page_config(page_title="GemCraft 庫存管理系統", layout="wide")
st.title("💎 GemCraft 庫存管理系統")

with st.sidebar:
    st.header("功能導航")
    page = st.radio("前往", ["📦 庫存管理與進貨", "📜 進貨紀錄查詢", "🧮 設計與成本計算"])
    st.divider()
    st.header("💾 資料備份")
    
    if not st.session_state['inventory'].empty:
        csv = st.session_state['inventory'].to_csv(index=False).encode('utf-8-sig')
        st.download_button("📥 下載庫存總表 (CSV)", csv, f'inventory_summary_{date.today()}.csv', "text/csv")
    
    if not st.session_state['history'].empty:
        hist_csv = st.session_state['history'].to_csv(index=False).encode('utf-8-sig')
        st.download_button("📥 下載進貨明細 (CSV)", hist_csv, f'purchase_history_{date.today()}.csv', "text/csv")
    
    uploaded_file = st.file_uploader("📤 上傳復原庫存 (CSV/Excel)", type=['csv', 'xlsx', 'xls'])
    if uploaded_file is not None:
        try:
            if uploaded_file.name.endswith('.csv'):
                uploaded_df = pd.read_csv(uploaded_file)
            else:
                try:
                    uploaded_df = pd.read_excel(uploaded_file)
                except ImportError:
                    st.error("🛑 缺少 openpyxl")
                    st.stop()
            
            uploaded_df = normalize_columns(uploaded_df)
            missing_cols = set(COLUMNS) - set(uploaded_df.columns)
            
            if not missing_cols:
                uploaded_df['編號'] = uploaded_df['編號'].astype(str)
                uploaded_df['單顆成本'] = pd.to_numeric(uploaded_df['單顆成本'], errors='coerce').fillna(0)
                uploaded_df['庫存(顆)'] = pd.to_numeric(uploaded_df['庫存(顆)'], errors='coerce').fillna(0)
                uploaded_df = uploaded_df[COLUMNS]
                if st.button("⚠️ 確認覆蓋庫存總表"):
                    st.session_state['inventory'] = uploaded_df
                    save_inventory_to_csv()
                    st.success("資料已還原！")
                    st.rerun()
            else:
                st.error(f"格式錯誤！缺少欄位：\n{', '.join(missing_cols)}")
        except Exception as e:
            st.error(f"讀取失敗: {e}")

# ------------------------------------------
# 頁面 A: 庫存管理
# ------------------------------------------
if page == "📦 庫存管理與進貨":
    st.subheader("📦 庫存管理")
    
    tab_restock, tab_new, tab_edit = st.tabs(["🔄 已有商品補貨", "✨ 建立新商品", "🛠️ 修改/刪除商品"])

    # === Tab 1: 補貨 ===
    with tab_restock:
        st.caption("針對已經存在的商品編號進行數量追加。")
        inventory_df = st.session_state['inventory']
        if not inventory_df.empty:
            restock_df = inventory_df.copy()
            restock_df['label'] = restock_df.apply(make_inventory_label, axis=1)
            
            c_re1, c_re2 = st.columns([2, 1])
            with c_re1:
                selected_restock_label = st.selectbox("選擇要補貨的商品", restock_df['label'].tolist())
            
            target_row = restock_df[restock_df['label'] == selected_restock_label].iloc[0]
            
            with st.form("restock_form"):
                st.markdown("**進貨資訊**")
                batch_id = st.text_input("進貨單號 (留空則自動產生)", placeholder="例如：IN-20241211-01")

                c1, c2, c3 = st.columns(3)
                with c1: qty = st.number_input("補貨數量 (顆)", min_value=1, value=10)
                with c2: price = st.number_input("本次進貨總價 ($)", min_value=0, value=0)
                with c3: p_date = st.date_input("進貨日期", value=date.today())
                
                sup_opts = get_dynamic_options('進貨廠商', DEFAULT_SUPPLIERS)
                try: def_idx = sup_opts.index(target_row['進貨廠商']) 
                except: def_idx = 0
                sup_sel = st.selectbox("廠商", sup_opts, index=def_idx)
                
                final_sup = st.text_input("↳ 輸入新廠商名稱") if sup_sel == "➕ 手動輸入新資料" else sup_sel

                if st.form_submit_button("📦 確認補貨"):
                    if not final_sup:
                        st.error("請輸入廠商名稱")
                    else:
                        if not batch_id:
                            batch_id = f"IN-{datetime.now().strftime('%Y%m%d-%H%M%S')}"

                        old_qty = target_row['庫存(顆)']
                        old_cost = target_row['單顆成本']
                        old_val = old_qty * old_cost
                        new_unit_cost = price / qty if qty > 0 else 0
                        final_qty = old_qty + qty
                        final_avg_cost = (old_val + price) / final_qty if final_qty > 0 else 0
                        
                        idx = inventory_df[inventory_df['編號'] == target_row['編號']].index[0]
                        st.session_state['inventory'].at[idx, '庫存(顆)'] = final_qty
                        st.session_state['inventory'].at[idx, '單顆成本'] = final_avg_cost
                        st.session_state['inventory'].at[idx, '進貨日期'] = p_date
                        st.session_state['inventory'].at[idx, '進貨廠商'] = final_sup
                        
                        log = {
                            '紀錄時間': datetime.now().strftime("%Y-%m-%d %H:%M"), 
                            '單號': batch_id, '動作': '補貨',
                            '編號': target_row['編號'], '分類': target_row['分類'], '名稱': target_row['名稱'],
                            '寬度mm': target_row['寬度mm'], '長度mm': target_row['長度mm'], '形狀': target_row['形狀'],
                            '廠商': final_sup, '進貨數量': qty, '進貨總價': price, '單價': new_unit_cost
                        }
                        st.session_state['history'] = pd.concat([st.session_state['history'], pd.DataFrame([log])], ignore_index=True)
                        save_inventory_to_csv()
                        st.success(f"已補貨！單號：{batch_id}，目前庫存 {final_qty} 顆")
                        time.sleep(1)
                        st.rerun()
        else:
            st.info("目前無庫存。")

    # === Tab 2: 建立新商品 ===
    with tab_new:
        with st.container():
            st.markdown("##### 1. 基本資料")
            c1, c2 = st.columns([1, 1.5])
            with c1: new_cat = st.selectbox("分類", ["天然石", "配件", "耗材"])
            with c2:
                existing_names = []
                if not st.session_state['inventory'].empty:
                    cat_df = st.session_state['inventory'][st.session_state['inventory']['分類'] == new_cat]
                    existing_names = sorted(cat_df['名稱'].dropna().unique().astype(str).tolist())
                name_opts = ["➕ 手動輸入新名稱"] + existing_names
                name_sel = st.selectbox("名稱", name_opts)
                final_name = st.text_input("↳ 輸入新名稱") if name_sel == "➕ 手動輸入新名稱" else name_sel

            st.markdown("##### 2. 規格尺寸 (mm)")
            c3a, c3b = st.columns(2)
            with c3a:
                existing_widths = []
                if not st.session_state['inventory'].empty:
                    widths_raw = st.session_state['inventory']['寬度mm'].dropna().unique()
                    try: existing_widths = sorted({float(x) for x in widths_raw})
                    except: existing_widths = []
                w_sel = st.selectbox("寬度/直徑", ["➕ 手動輸入"] + existing_widths)
                final_w = st.number_input("↳ 輸入寬度", min_value=0.0, step=0.5, format="%.1f") if w_sel == "➕ 手動輸入" else float(w_sel)
            with c3b:
                final_l = st.number_input("長度 (圓珠可不填)", min_value=0.0, step=0.5, format="%.1f")
                if final_l == 0.0 and final_w > 0: st.caption(f"預設為 {final_w}")

        prev_row = None
        if final_name and not st.session_state['inventory'].empty:
            same_name_df = st.session_state['inventory'][(st.session_state['inventory']['分類'] == new_cat) & (st.session_state['inventory']['名稱'] == final_name)]
            if not same_name_df.empty:
                tmp = same_name_df.copy()
                tmp['進貨日期_排序'] = pd.to_datetime(tmp['進貨日期'], errors='coerce')
                prev_row = tmp.sort_values('進貨日期_排序', ascending=False).iloc[0]

        with st.form("add_new"):
            st.markdown("##### 3. 詳細資訊")
            batch_id_new = st.text_input("進貨單號 (留空則自動產生)", placeholder="例如：IN-20241211-01")

            shape_opts = get_dynamic_options('形狀', DEFAULT_SHAPES)
            elem_opts = get_dynamic_options('五行', DEFAULT_ELEMENTS)
            sup_opts = get_dynamic_options('進貨廠商', DEFAULT_SUPPLIERS)
            
            def get_idx(opts, val):
                try: return opts.index(val)
                except: return 1 if len(opts) > 1 else 0

            idx_s = get_idx(shape_opts, prev_row['形狀']) if prev_row is not None else 1
            idx_e = get_idx(elem_opts, prev_row['五行']) if prev_row is not None else 1
            idx_p = get_idx(sup_opts, prev_row['進貨廠商']) if prev_row is not None else 1

            c4, c5, c6 = st.columns(3)
            with c4: s_sel = st.selectbox("形狀", shape_opts, index=idx_s)
            with c5: e_sel = st.selectbox("五行", elem_opts, index=idx_e)
            with c6: p_sel = st.selectbox("廠商", sup_opts, index=idx_p)
            
            mc1, mc2, mc3 = st.columns(3)
            final_shape = mc1.text_input("↳ 新形狀") if s_sel == "➕ 手動輸入新資料" else s_sel
            final_elem = mc2.text_input("↳ 新五行") if e_sel == "➕ 手動輸入新資料" else e_sel
            final_sup = mc3.text_input("↳ 新廠商") if p_sel == "➕ 手動輸入新資料" else p_sel

            c7, c8, c9 = st.columns(3)
            with c7: price = st.number_input("進貨總價", 0)
            with c8: qty = st.number_input("進貨數量", 1)
            with c9: p_date = st.date_input("進貨日期", value=date.today())
            
            if st.form_submit_button("➕ 確認新增入庫", type="primary"):
                if not all([final_name, final_shape, final_elem, final_sup]):
                    st.error("❌ 請填寫完整欄位")
                else:
                    if not batch_id_new:
                        batch_id_new = f"IN-{datetime.now().strftime('%Y%m%d-%H%M%S')}"

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
                        '單號': batch_id_new, '動作': '進貨',
                        '編號': new_id, '分類': new_cat, '名稱': final_name,
                        '寬度mm': final_w, '長度mm': save_l, '形狀': final_shape,
                        '廠商': final_sup, '進貨數量': qty, '進貨總價': price, '單價': unit_cost
                    }
                    st.session_state['history'] = pd.concat([st.session_state['history'], pd.DataFrame([log])], ignore_index=True)
                    save_inventory_to_csv()
                    st.success(f"✅ 已新增：{final_name} - {new_id} (單號: {batch_id_new})")
                    time.sleep(1)
                    st.rerun()

    # === Tab 3: 修改/刪除 ===
    with tab_edit:
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
                eshape = em1.text_input("↳ 新形狀") if eshp_sel == "➕ 手動輸入新資料" else eshp_sel
                eelem = em2.text_input("↳ 新五行") if eelm_sel == "➕ 手動輸入新資料" else eelm_sel
                esup = em3.text_input("↳ 新廠商") if esup_sel == "➕ 手動輸入新資料" else esup_sel

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
                        
                        log = {
                            '紀錄時間': datetime.now().strftime("%Y-%m-%d %H:%M"), 
                            '單號': 'EDIT', '動作': '修改資料',
                            '編號': orig_row['編號'], '分類': orig_row['分類'], '名稱': ename,
                            '寬度mm': ewidth, '長度mm': elength, '形狀': eshape,
                            '廠商': esup, '進貨數量': 0, '進貨總價': 0, '單價': ecost
                        }
                        st.session_state['history'] = pd.concat([st.session_state['history'], pd.DataFrame([log])], ignore_index=True)
                        save_inventory_to_csv()
                        st.success("✅ 更新成功")
                        time.sleep(1)
                        st.rerun()

                with bt2:
                    if st.form_submit_button("🗑️ 刪除商品", type="primary"):
                        st.session_state['inventory'] = st.session_state['inventory'].drop(orig_idx).reset_index(drop=True)
                        save_inventory_to_csv()
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
            save_inventory_to_csv()
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
    st.subheader("📜 進貨與異動紀錄")
    cols = st.session_state['history'].columns.tolist()
    if '單號' in cols:
        cols.remove('單號')
        cols.insert(1, '單號')
        st.dataframe(st.session_state['history'][cols], use_container_width=True)
    else:
        st.dataframe(st.session_state['history'], use_container_width=True)

# ------------------------------------------
# 頁面 C: 設計與成本
# ------------------------------------------
elif page == "🧮 設計與成本計算":
    st.subheader("🧮 手鍊設計成本試算")
    all_items = st.session_state['inventory']

    if not all_items.empty:
        uniq_ele = sorted(all_items['五行'].astype(str).unique().tolist())
        st.write("👇 **第一步：選擇五行屬性 (可複選)**")
        sel_ele = st.multiselect("五行", uniq_ele, default=uniq_ele)
        if not sel_ele: sel_ele = uniq_ele
        
        filt_items = all_items[all_items['五行'].isin(sel_ele)].sort_values(by=['五行', '名稱', '編號'])

        st.divider()

        if not filt_items.empty:
            temp_df = filt_items.copy()
            temp_df['label'] = temp_df.apply(make_design_label, axis=1)
            
            c_sel, c_qty, c_btn = st.columns([3, 1, 1])
            with c_sel:
                sel_label = st.selectbox(f"👇 選擇珠子 (篩選：{', '.join(sel_ele)})", temp_df['label'].tolist())
            with c_qty:
                in_qty = st.number_input("數量", min_value=1, value=1)
            with c_btn:
                st.write("") 
                st.write("") 
                if st.button("⬇️ 加入清單", use_container_width=True, type="primary"):
                    row = temp_df[temp_df['label'] == sel_label].iloc[0]
                    subtotal = row['單顆成本'] * in_qty
                    st.session_state['current_design'].append({
                        '編號': row['編號'], '分類': row['五行'], '名稱': row['名稱'],
                        '形狀': row['形狀'], '規格': f"{row['寬度mm']}x{row['長度mm']}",
                        '單價': row['單顆成本'], '數量': in_qty, '小計': subtotal
                    })
                    st.success(f"已加入 {in_qty} 顆 {row['名稱']}")

            st.divider()
            st.markdown("##### 📝 目前設計清單")
            
            if st.session_state['current_design']:
                h1, h2, h3, h4, h5, h6 = st.columns([1, 2, 2, 1.5, 1, 0.8])
                h1.markdown("**編號**")
                h2.markdown("**名稱**")
                h3.markdown("**規格**")
                h4.markdown("**單價**")
                h5.markdown("**數量**")
                h6.markdown("**移除**")
                st.divider()

                design_list = st.session_state['current_design']
                rows_to_del = []
                mat_cost = 0

                for i, item in enumerate(design_list):
                    c1, c2, c3, c4, c5, c6 = st.columns([1, 2, 2, 1.5, 1, 0.8])
                    with c1: st.write(item['編號'])
                    with c2: st.write(f"{item['名稱']} ({item['分類']})")
                    with c3: st.write(f"{item['形狀']} {item['規格']}")
                    with c4: st.write(f"${item['單價']:.1f}")
                    with c5: st.write(f"{item['數量']}")
                    with c6:
                        if st.button("🗑️", key=f"del_{i}"): rows_to_del.append(i)
                    mat_cost += item['小計']

                if rows_to_del:
                    for i in sorted(rows_to_del, reverse=True):
                        del st.session_state['current_design'][i]
                    st.rerun()

                st.divider()
                st.markdown("##### 💰 額外成本設定")
                lc, mc = st.columns(2)
                with lc: labor = st.number_input("工資 ($)", min_value=0, value=0, step=10)
                with mc: misc = st.number_input("雜支/包材/運費 ($)", min_value=0, value=0, step=5)

                # ★★★ 新公式應用 ★★★
                # 總成本 (含工雜) - 用於參考
                total_cost_base = mat_cost + labor + misc
                
                # 建議售價公式
                # x3 = (材料 * 3) + 工資 + 雜支
                price_x3 = (mat_cost * 3) + labor + misc
                # x5 = (材料 * 5) + 工資 + 雜支
                price_x5 = (mat_cost * 5) + labor + misc
                
                tot_qty = sum(x['數量'] for x in design_list)
                
                st.info(f"💎 材料費: ${mat_cost:.1f} + 工資: ${labor} + 雜支: ${misc}")
                
                m1, m2, m3, m4 = st.columns(4)
                m1.metric("總顆數", f"{tot_qty} 顆")
                m2.metric("總成本 (成本價)", f"${total_cost_base:.1f}")
                m3.metric("建議售價 (材料x3+工雜)", f"${price_x3:.0f}")
                m4.metric("建議售價 (材料x5+工雜)", f"${price_x5:.0f}")
                
                st.divider()
                act_c1, act_c2 = st.columns([3, 1])
                
                with act_c1:
                    sales_order_id = st.text_input("自訂訂單編號 (留空則自動產生)", placeholder="例如：客戶名或蝦皮單號")
                
                with act_c2:
                    if st.button("✅ 確認售出 (扣庫存)", type="primary", use_container_width=True):
                        if not sales_order_id:
                            sales_order_id = f"OUT-{datetime.now().strftime('%Y%m%d-%H%M%S')}"

                        for item in design_list:
                            mask = st.session_state['inventory']['編號'] == item['編號']
                            if mask.any():
                                idx = st.session_state['inventory'][mask].index[0]
                                current_stock = st.session_state['inventory'].at[idx, '庫存(顆)']
                                st.session_state['inventory'].at[idx, '庫存(顆)'] = current_stock - item['數量']
                                
                                log = {
                                    '紀錄時間': datetime.now().strftime("%Y-%m-%d %H:%M"),
                                    '單號': sales_order_id,
                                    '動作': '售出使用',
                                    '編號': item['編號'], '分類': item['分類'], '名稱': item['名稱'],
                                    '寬度mm': 0, '長度mm': 0, '形狀': item['形狀'],
                                    '廠商': '自用/售出', '進貨數量': -item['數量'], 
                                    '進貨總價': 0, '單價': item['單價']
                                }
                                st.session_state['history'] = pd.concat([st.session_state['history'], pd.DataFrame([log])], ignore_index=True)
                        
                        save_inventory_to_csv()
                        st.session_state['current_design'] = []
                        st.success(f"🎉 已成功扣除庫存！單號：{sales_order_id}")
                        time.sleep(1)
                        st.rerun()

                if st.button("🗑️ 清空所有清單", type="secondary"):
                    st.session_state['current_design'] = []
                    st.rerun()

            else:
                st.info("尚未加入任何配件。")
        else:
            st.warning("查無符合條件的庫存。")
    else:
        st.info("庫存為空。")
