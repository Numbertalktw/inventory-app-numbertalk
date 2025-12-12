import streamlit as st
import pandas as pd
from datetime import date, datetime
import os
import time

# ==========================================
# 1. 核心設定區
# ==========================================

# 系統標準欄位 (確保 '尺寸規格' 存在以修復 KeyError)
COLUMNS = [
    '編號', '分類', '系列', '名稱', '尺寸規格', # 識別欄位
    '寬度mm', '長度mm', '形狀', '五行',       # 實體規格
    '進貨總價', '進貨數量(顆)', '進貨日期', '進貨廠商', 
    '庫存(顆)', '單顆成本'
]

# 歷史紀錄欄位
HISTORY_COLUMNS = [
    '紀錄時間', '單號', '動作', '編號', '分類', '名稱', '尺寸規格', 
    '廠商', '進貨數量', '進貨總價', '單價'
]

DEFAULT_CSV_FILE = 'inventory_backup_v2.csv'
RULES_FILE = 'coding_rules.xlsx'  # 編碼規則檔名

# 預設的一些基本選項 (當沒有 Excel 規則時的備案)
DEFAULT_SUPPLIERS = ["廠商A", "廠商B", "自用"]
DEFAULT_SHAPES = ["圓珠", "切角", "鑽切", "圓筒", "不規則"]
DEFAULT_ELEMENTS = ["金", "木", "水", "火", "土"]

# ==========================================
# 2. 核心邏輯函式
# ==========================================

def save_inventory():
    """儲存庫存到 CSV"""
    try:
        if 'inventory' in st.session_state:
            st.session_state['inventory'].to_csv(DEFAULT_CSV_FILE, index=False, encoding='utf-8-sig')
    except Exception as e:
        st.error(f"儲存失敗: {e}")

def load_coding_rules(uploaded_file=None):
    """讀取編碼規則 Excel，回傳四個字典與DataFrame"""
    rules = {
        'cat': {}, 'series': {}, 'name': {}, 'size': {}
    }
    dfs = {}
    
    try:
        # 如果有上傳檔案就用上傳的，否則嘗試讀取本地檔案
        source = uploaded_file if uploaded_file else (RULES_FILE if os.path.exists(RULES_FILE) else None)
        
        if source:
            # 讀取 Excel (假設第一行是標題)
            df = pd.read_excel(source, header=0)
            
            # 清理欄位名稱 (去除空白)
            df.columns = [str(c).strip() for c in df.columns]
            
            # 依據您的截圖結構截取各部分的對照表 (去除空值)
            # A-B: 類別 (Category)
            if df.shape[1] >= 2:
                cat_df = df.iloc[:, [0, 1]].dropna().astype(str)
                # 排除標題列誤讀 (如果第一列是資料)
                rules['cat'] = dict(zip(cat_df.iloc[:, 0], cat_df.iloc[:, 1]))
                dfs['cat'] = cat_df
            
            # C-D: 系列 (Series)
            if df.shape[1] >= 4:
                series_df = df.iloc[:, [2, 3]].dropna().astype(str)
                rules['series'] = dict(zip(series_df.iloc[:, 0], series_df.iloc[:, 1]))
                dfs['series'] = series_df
                
            # E-F: 名稱 (Name)
            if df.shape[1] >= 6:
                name_df = df.iloc[:, [4, 5]].dropna().astype(str)
                rules['name'] = dict(zip(name_df.iloc[:, 0], name_df.iloc[:, 1]))
                dfs['name'] = name_df
                
            # G-H: 尺寸 (Size)
            if df.shape[1] >= 8:
                size_df = df.iloc[:, [6, 7]].dropna().astype(str)
                rules['size'] = dict(zip(size_df.iloc[:, 0], size_df.iloc[:, 1]))
                dfs['size'] = size_df
                
            return rules, dfs
    except Exception as e:
        st.error(f"讀取規則檔失敗: {e}")
        
    return rules, dfs

def get_rule_options(rule_dict):
    """將規則字典轉換為下拉選單選項"""
    options = [f"{k} ({v})" for k, v in rule_dict.items()]
    return ["➕ 手動輸入/新增"] + sorted(options)

def parse_selection(selection, rule_dict):
    """解析下拉選單的選擇，回傳 (名稱, 代號)"""
    if selection == "➕ 手動輸入/新增" or not selection:
        return None, None
    
    # 格式通常是 "名稱 (代號)"
    try:
        # 從最後一個 " (" 切割，避免名稱本身包含括號
        name = selection.rsplit(' (', 1)[0]
        code = selection.rsplit(' (', 1)[1][:-1]
        return name, code
    except:
        return selection, ""

def normalize_columns(df):
    """標準化庫存欄位，確保欄位存在且名稱正確"""
    # 舊欄位對應修正
    rename_map = {
        '尺寸': '尺寸規格', '規格': '尺寸規格', 'Size': '尺寸規格',
        '寬度': '寬度mm', 'Width': '寬度mm',
        'Name': '名稱', 'Category': '分類',
        'Code': '編號', 'ID': '編號'
    }
    df = df.rename(columns=rename_map)
    
    # 補齊缺少欄位
    for col in COLUMNS:
        if col not in df.columns:
            if 'mm' in col or '價' in col or '數量' in col or '成本' in col:
                df[col] = 0
            else:
                df[col] = ""
    
    return df[COLUMNS]

# ==========================================
# 3. 初始化
# ==========================================

if 'inventory' not in st.session_state:
    if os.path.exists(DEFAULT_CSV_FILE):
        try:
            df = pd.read_csv(DEFAULT_CSV_FILE)
            st.session_state['inventory'] = normalize_columns(df)
        except:
            st.session_state['inventory'] = pd.DataFrame(columns=COLUMNS)
    else:
        st.session_state['inventory'] = pd.DataFrame(columns=COLUMNS)

if 'history' not in st.session_state:
    st.session_state['history'] = pd.DataFrame(columns=HISTORY_COLUMNS)

if 'current_design' not in st.session_state:
    st.session_state['current_design'] = []

# 載入編碼規則 (Session State 快取)
if 'coding_rules' not in st.session_state:
    st.session_state['coding_rules'], st.session_state['rule_dfs'] = load_coding_rules()

# ==========================================
# 4. UI 介面
# ==========================================

st.set_page_config(page_title="GemCraft 庫存管理系統 (長貨號版)", layout="wide")
st.title("💎 GemCraft 庫存管理系統")

with st.sidebar:
    st.header("功能導航")
    page = st.radio("前往", ["📦 庫存管理與進貨", "⚙️ 編碼規則設定", "📜 進貨紀錄查詢", "🧮 設計與成本計算"])
    st.divider()
    
    # 備份功能
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
        except Exception as e:
            st.error(f"讀取失敗: {e}")

# ------------------------------------------
# 頁面: 編碼規則設定 (新增)
# ------------------------------------------
if page == "⚙️ 編碼規則設定":
    st.subheader("⚙️ 商品編碼規則管理")
    st.info("💡 請上傳 `貨號分類.xlsx`，系統將自動分析 A~H 欄位規則 (類別-系列-名稱-尺寸)。")
    
    # 檔案上傳區
    uploaded_rules = st.file_uploader("上傳規則檔 (Excel)", type=['xlsx', 'xls'])
    
    if uploaded_rules:
        rules, dfs = load_coding_rules(uploaded_rules)
        if rules['cat']:
            st.session_state['coding_rules'] = rules
            st.session_state['rule_dfs'] = dfs
            # 存檔供下次使用 (若是本地環境)
            try:
                with open(RULES_FILE, "wb") as f:
                    f.write(uploaded_rules.getbuffer())
                st.success("✅ 規則檔已更新並儲存！")
            except:
                st.success("✅ 規則已暫時載入 (無法寫入伺服器檔案，重新整理需重傳)")
        else:
            st.error("❌ 讀取失敗，請確認 Excel 欄位順序是否正確。")

    st.divider()
    
    # 顯示目前規則預覽
    st.markdown("##### 🔍 目前生效的編碼規則")
    if st.session_state.get('rule_dfs'):
        dfs = st.session_state['rule_dfs']
        c1, c2, c3, c4 = st.columns(4)
        
        with c1:
            st.markdown("**1. 類別 (A/B欄)**")
            if 'cat' in dfs: st.dataframe(dfs['cat'], hide_index=True)
        with c2:
            st.markdown("**2. 系列 (C/D欄)**")
            if 'series' in dfs: st.dataframe(dfs['series'], hide_index=True)
        with c3:
            st.markdown("**3. 名稱 (E/F欄)**")
            if 'name' in dfs: st.dataframe(dfs['name'], hide_index=True)
        with c4:
            st.markdown("**4. 尺寸 (G/H欄)**")
            if 'size' in dfs: st.dataframe(dfs['size'], hide_index=True)
    else:
        st.warning("尚未設定規則，請上傳 Excel 檔。")

# ------------------------------------------
# 頁面: 庫存管理
# ------------------------------------------
elif page == "📦 庫存管理與進貨":
    st.subheader("📦 庫存管理")
    
    tab1, tab2, tab3 = st.tabs(["🔄 舊品補貨", "✨ 建立新商品 (長貨號)", "🛠️ 修改與刪除"])
    
    # === Tab 1: 補貨 (簡單版) ===
    with tab1:
        inv_df = st.session_state['inventory']
        if not inv_df.empty:
            # 製作選單: 顯示 "編號 | 名稱 規格"
            # 修復 KeyError: 確保欄位存在並轉為字串
            inv_df['label'] = inv_df.apply(
                lambda x: f"{str(x['編號'])} | {str(x['名稱'])} {str(x['尺寸規格'])}", axis=1
            )
            target_label = st.selectbox("選擇商品", inv_df['label'].tolist())
            
            target_row = inv_df[inv_df['label'] == target_label].iloc[0]
            target_idx = inv_df[inv_df['label'] == target_label].index[0]
            
            with st.form("restock"):
                st.write(f"目前庫存: **{target_row['庫存(顆)']}** 顆")
                c1, c2, c3 = st.columns(3)
                batch_no = c1.text_input("進貨單號 (選填)", placeholder="例如：IN-20251212")
                qty = c2.number_input("進貨數量", min_value=1, value=10)
                cost = c3.number_input("進貨總價", min_value=0, value=0)
                
                if st.form_submit_button("📦 確認補貨"):
                    new_qty = target_row['庫存(顆)'] + qty
                    # 移動加權平均成本
                    old_val = target_row['庫存(顆)'] * target_row['單顆成本']
                    new_avg = (old_val + cost) / new_qty if new_qty > 0 else 0
                    
                    st.session_state['inventory'].at[target_idx, '庫存(顆)'] = new_qty
                    st.session_state['inventory'].at[target_idx, '單顆成本'] = new_avg
                    st.session_state['inventory'].at[target_idx, '進貨日期'] = date.today()
                    
                    # 紀錄
                    log = {
                        '紀錄時間': datetime.now().strftime("%Y-%m-%d %H:%M"),
                        '單號': batch_no if batch_no else f"AUTO-{int(time.time())}",
                        '動作': '補貨',
                        '編號': target_row['編號'], '分類': target_row['分類'], '名稱': target_row['名稱'],
                        '尺寸規格': target_row['尺寸規格'], '廠商': target_row['進貨廠商'],
                        '進貨數量': qty, '進貨總價': cost, '單價': cost/qty if qty>0 else 0
                    }
                    st.session_state['history'] = pd.concat([st.session_state['history'], pd.DataFrame([log])], ignore_index=True)
                    save_inventory()
                    st.success(f"補貨成功！目前庫存 {new_qty}")
                    time.sleep(1)
                    st.rerun()
        else:
            st.info("尚無庫存資料。")

    # === Tab 2: 建立新商品 (核心功能) ===
    with tab2:
        st.markdown("##### 🏗️ 產生長貨號：類別-系列-名稱-尺寸")
        
        rules = st.session_state.get('coding_rules', {'cat':{}, 'series':{}, 'name':{}, 'size':{}})
        
        col1, col2 = st.columns(2)
        col3, col4 = st.columns(2)
        
        # 1. 類別選擇
        with col1:
            cat_opts = get_rule_options(rules['cat'])
            sel_cat = st.selectbox("1. 商品類別", cat_opts)
            name_cat, code_cat = parse_selection(sel_cat, rules['cat'])
            
            if not code_cat: # 手動模式
                c_m1, c_m2 = st.columns([2,1])
                name_cat = c_m1.text_input("輸入類別名稱", key="m_cat_n")
                code_cat = c_m2.text_input("代號 (如:SB)", key="m_cat_c").upper()

        # 2. 系列選擇
        with col2:
            series_opts = get_rule_options(rules['series'])
            sel_series = st.selectbox("2. 商品系列", series_opts)
            name_series, code_series = parse_selection(sel_series, rules['series'])
            
            if not code_series:
                c_m3, c_m4 = st.columns([2,1])
                name_series = c_m3.text_input("輸入系列名稱", key="m_ser_n")
                code_series = c_m4.text_input("代號 (如:S01)", key="m_ser_c").upper()

        # 3. 名稱選擇
        with col3:
            name_opts = get_rule_options(rules['name'])
            sel_name = st.selectbox("3. 商品名稱", name_opts)
            name_prod, code_prod = parse_selection(sel_name, rules['name'])
            
            if not code_prod:
                c_m5, c_m6 = st.columns([2,1])
                name_prod = c_m5.text_input("輸入商品名稱", key="m_nm_n")
                code_prod = c_m6.text_input("代號 (如:A01)", key="m_nm_c").upper()

        # 4. 尺寸選擇
        with col4:
            size_opts = get_rule_options(rules['size'])
            sel_size = st.selectbox("4. 尺寸/重量", size_opts)
            name_size, code_size = parse_selection(sel_size, rules['size'])
            
            if not code_size:
                c_m7, c_m8 = st.columns([2,1])
                name_size = c_m7.text_input("輸入尺寸規格", key="m_sz_n")
                code_size = c_m8.text_input("代號 (如:AA36)", key="m_sz_c").upper()

        # --- 產生預覽 ---
        full_id = ""
        if code_cat and code_series and code_prod and code_size:
            # 組合邏輯：依照您的規則 類別-系列-名稱-尺寸
            full_id = f"{code_cat}-{code_series}-{code_prod}-{code_size}"
            st.success(f"🎫 預覽長貨號：**{full_id}**")
            st.caption(f"全名：{name_cat} {name_series} {name_prod} {name_size}")
        else:
            st.warning("請完整選擇 4 個欄位以產生貨號")

        st.divider()
        
        # --- 進貨數值填寫 ---
        with st.form("new_item_form"):
            st.markdown("##### 📝 進貨數值與詳細屬性")
            f1, f2, f3 = st.columns(3)
            with f1: batch_no = st.text_input("進貨單號", placeholder="Auto")
            with f2: qty = st.number_input("數量", 1)
            with f3: cost = st.number_input("總價", 0)
            
            f4, f5, f6 = st.columns(3)
            with f4: supplier = st.selectbox("廠商", DEFAULT_SUPPLIERS + ["其他"])
            with f5: shape = st.selectbox("形狀 (選填)", DEFAULT_SHAPES)
            with f6: element = st.selectbox("五行 (選填)", DEFAULT_ELEMENTS)
            
            # 隱藏欄位 (為了計算)
            width = st.number_input("寬度mm (選填)", 0.0)
            length = st.number_input("長度mm (選填)", 0.0)

            if st.form_submit_button("🚀 確認建立商品"):
                if not full_id:
                    st.error("貨號不完整，無法建立")
                else:
                    # 檢查重複
                    if full_id in st.session_state['inventory']['編號'].values:
                        st.error("❌ 此貨號已存在！請至「舊品補貨」分頁操作。")
                    else:
                        unit_cost = cost / qty if qty > 0 else 0
                        new_data = {
                            '編號': full_id,
                            '分類': name_cat, '系列': name_series,
                            '名稱': name_prod, '尺寸規格': name_size,
                            '寬度mm': width, '長度mm': length,
                            '形狀': shape, '五行': element,
                            '進貨總價': cost, '進貨數量(顆)': qty,
                            '進貨日期': date.today(), '進貨廠商': supplier,
                            '庫存(顆)': qty, '單顆成本': unit_cost
                        }
                        
                        st.session_state['inventory'] = pd.concat(
                            [st.session_state['inventory'], pd.DataFrame([new_data])], 
                            ignore_index=True
                        )
                        
                        # 紀錄
                        log = {
                            '紀錄時間': datetime.now().strftime("%Y-%m-%d %H:%M"),
                            '單號': batch_no if batch_no else "NEW-ITEM",
                            '動作': '新建立',
                            '編號': full_id, '分類': name_cat, '名稱': name_prod,
                            '尺寸規格': name_size, '廠商': supplier,
                            '進貨數量': qty, '進貨總價': cost, '單價': unit_cost
                        }
                        st.session_state['history'] = pd.concat([st.session_state['history'], pd.DataFrame([log])], ignore_index=True)
                        save_inventory()
                        st.success(f"成功建立！貨號：{full_id}")
                        time.sleep(1)
                        st.rerun()

    # === Tab 3: 修改與刪除 ===
    with tab3:
        st.markdown("##### 🛠️ 資料修正")
        inv = st.session_state['inventory']
        if not inv.empty:
            edit_id = st.selectbox("選擇要修改的貨號", inv['編號'].tolist())
            idx = inv[inv['編號'] == edit_id].index[0]
            row = inv.iloc[idx]
            
            with st.form("edit_form"):
                st.info(f"正在編輯：{row['名稱']} ({row['尺寸規格']})")
                c1, c2 = st.columns(2)
                new_stock = c1.number_input("修正庫存數量", value=int(row['庫存(顆)']))
                new_cost = c2.number_input("修正單顆成本", value=float(row['單顆成本']))
                
                c3, c4 = st.columns(2)
                if c3.form_submit_button("💾 更新資料"):
                    st.session_state['inventory'].at[idx, '庫存(顆)'] = new_stock
                    st.session_state['inventory'].at[idx, '單顆成本'] = new_cost
                    save_inventory()
                    st.success("更新完成")
                    st.rerun()
                    
                if c4.form_submit_button("🗑️ 刪除此商品", type="primary"):
                    st.session_state['inventory'] = inv.drop(idx).reset_index(drop=True)
                    save_inventory()
                    st.success("已刪除")
                    st.rerun()

    # 庫存列表顯示
    st.divider()
    st.subheader("📋 庫存總表")
    st.dataframe(st.session_state['inventory'], use_container_width=True)

# ------------------------------------------
# 頁面: 紀錄查詢
# ------------------------------------------
elif page == "📜 進貨紀錄查詢":
    st.subheader("📜 歷史紀錄")
    st.dataframe(st.session_state['history'], use_container_width=True)

# ------------------------------------------
# 頁面: 設計與成本
# ------------------------------------------
elif page == "🧮 設計與成本計算":
    st.subheader("🧮 成本試算與報價")
    
    inv = st.session_state['inventory']
    if not inv.empty:
        # 選單製作
        inv['disp'] = inv.apply(lambda x: f"【{x['分類']}】{x['名稱']} ({x['尺寸規格']}) | ${x['單顆成本']:.1f}", axis=1)
        
        c1, c2, c3 = st.columns([3, 1, 1])
        item_sel = c1.selectbox("選擇材料", inv['disp'].tolist())
        qty_sel = c2.number_input("數量", 1)
        
        if c3.button("⬇️ 加入", use_container_width=True):
            row = inv[inv['disp'] == item_sel].iloc[0]
            st.session_state['current_design'].append({
                '編號': row['編號'], '名稱': row['名稱'], '規格': row['尺寸規格'],
                '單價': row['單顆成本'], '數量': qty_sel, 
                '小計': row['單顆成本'] * qty_sel
            })
            
        st.divider()
        
        # 清單顯示
        if st.session_state['current_design']:
            df_design = pd.DataFrame(st.session_state['current_design'])
            
            # 顯示表格
            st.table(df_design)
            
            # 移除功能
            if st.button("🗑️ 清除最後一項"):
                st.session_state['current_design'].pop()
                st.rerun()
            
            # 計算區
            mat_cost = df_design['小計'].sum()
            
            st.markdown("#### 💰 成本結構")
            c_labor, c_misc = st.columns(2)
            labor = c_labor.number_input("工資 ($)", 0, step=10)
            misc = c_misc.number_input("雜支/運費 ($)", 0, step=5)
            
            total_base = mat_cost + labor + misc
            price_x3 = (mat_cost * 3) + labor + misc
            price_x5 = (mat_cost * 5) + labor + misc
            
            st.info(f"基礎材料費: ${mat_cost:.1f}")
            
            m1, m2, m3 = st.columns(3)
            m1.metric("總成本", f"${total_base:.0f}")
            m2.metric("建議售價 (x3)", f"${price_x3:.0f}")
            m3.metric("建議售價 (x5)", f"${price_x5:.0f}")
            
            # 售出按鈕
            st.divider()
            sale_id = st.text_input("訂單編號", placeholder="例如: 蝦皮241212...")
            if st.button("✅ 確認售出 (扣除庫存)", type="primary"):
                if not sale_id: sale_id = f"S-{int(time.time())}"
                
                for item in st.session_state['current_design']:
                    # 扣庫存
                    idx = inv[inv['編號'] == item['編號']].index[0]
                    inv.at[idx, '庫存(顆)'] -= item['數量']
                    
                    # 寫紀錄
                    log = {
                        '紀錄時間': datetime.now().strftime("%Y-%m-%d %H:%M"),
                        '單號': sale_id, '動作': '售出',
                        '編號': item['編號'], '名稱': item['名稱'], 
                        '尺寸規格': item['規格'], '進貨數量': -item['數量'],
                        '進貨總價': 0, '單價': item['單價']
                    }
                    st.session_state['history'] = pd.concat([st.session_state['history'], pd.DataFrame([log])], ignore_index=True)
                
                save_inventory()
                st.session_state['current_design'] = []
                st.success(f"已完成售出扣帳！單號：{sale_id}")
                time.sleep(1)
                st.rerun()
