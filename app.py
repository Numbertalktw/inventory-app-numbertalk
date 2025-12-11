import streamlit as st
import pandas as pd
from datetime import date, datetime
import os
import time
import io

# ==========================================
# 1. 系統設定
# ==========================================

PAGE_TITLE = "製造庫存系統 (含成本權限控管)"
INVENTORY_FILE = 'inventory_secure_v1.csv'
HISTORY_FILE = 'history_secure_v1.csv'
ADMIN_PASSWORD = "8888"  # 設定管理員密碼

# 定義倉庫
WAREHOUSES = ["原物料倉", "半成品倉", "成品倉", "報廢倉"]

# --- 核心流水帳 (新增 '進貨總成本' 欄位，放在最後面以免影響舊格式) ---
HISTORY_COLUMNS = [
    '單據類型', '單號', '日期', '系列', '分類', '品名', '貨號', '批號',
    '倉庫', '數量', 'Key單者',
    '訂單單號', '出貨日期', '貨號備註', '運費', 
    '款項結清', '工資', '發票', '備註',
    '進貨總成本' # <--- 新增欄位 (敏感資料)
]

# --- 庫存狀態表 ---
INVENTORY_COLUMNS = [
    '貨號', '系列', '分類', '品名', 
    '總庫存', '均價', # <--- 均價是敏感資料，一般頁面需隱藏
    '庫存_原物料倉', '庫存_半成品倉', '庫存_成品倉', '庫存_報廢倉'
]

DEFAULT_SERIES = ["原料", "半成品", "成品", "包材"]
DEFAULT_CATEGORIES = ["天然石", "金屬配件", "線材", "包裝盒", "完成品"]
DEFAULT_KEYERS = ["Wen", "廠長", "倉管", "業務"]

# ==========================================
# 2. 核心函式
# ==========================================

def load_data():
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
                    hist_df[col] = "" if col != '進貨總成本' else 0
            hist_df = hist_df[HISTORY_COLUMNS]
            hist_df['數量'] = pd.to_numeric(hist_df['數量'], errors='coerce').fillna(0)
            hist_df['進貨總成本'] = pd.to_numeric(hist_df['進貨總成本'], errors='coerce').fillna(0)
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
    """
    重算庫存與移動平均成本
    """
    new_inv = current_inv_df.copy()
    
    # 重置數據
    cols_reset = ['總庫存', '均價'] + [f'庫存_{w}' for w in WAREHOUSES]
    for col in cols_reset:
        new_inv[col] = 0.0
    
    # 依商品重算
    for idx, row in new_inv.iterrows():
        sku = str(row['貨號'])
        target_hist = hist_df[hist_df['貨號'].astype(str) == sku]
        
        # 成本計算變數
        total_qty = 0       # 當前總庫存 (用於加權平均)
        total_value = 0.0   # 當前總價值
        
        # 分倉變數
        w_stock = {w: 0 for w in WAREHOUSES}
        
        for _, h_row in target_hist.iterrows():
            qty = float(h_row['數量'])
            cost_total = float(h_row['進貨總成本'])
            doc_type = str(h_row['單據類型'])
            w_name = str(h_row['倉庫'])
            if w_name not in WAREHOUSES: w_name = WAREHOUSES[0]
            
            # 邏輯：
            # 1. 進貨/入庫 -> 增加庫存，重新計算均價
            if doc_type in ['進貨', '製造入庫', '調整入庫']:
                # 只有當「有輸入成本」時，才影響均價計算
                if cost_total > 0:
                    total_value += cost_total
                    # 注意：若是製造入庫，通常成本來自原料扣除(BOM)，此處簡化為手動輸入或0
                
                # 若無輸入成本(例如補登前)，則僅增加數量，均價會被稀釋(或暫時不變，視會計準則)
                # 這裡採用簡單移動平均：(原總值 + 新進貨總值) / (原數量 + 新數量)
                # 若 cost_total 為 0，代表還沒補登，暫時不加價值，只加數量 -> 均價會暫時變低 (提醒要去補登)
                
                total_qty += qty
                if w_name in w_stock: w_stock[w_name] += qty
            
            # 2. 出貨/領料 -> 減少庫存，均價不變，總值減少
            elif doc_type in ['銷售出貨', '製造領料', '調整出庫']:
                # 出庫時，依據「當前均價」扣除價值
                current_avg = (total_value / total_qty) if total_qty > 0 else 0
                
                total_qty -= qty
                total_value -= (qty * current_avg)
                
                if w_name in w_stock: w_stock[w_name] -= qty

        # 更新 Inventory
        new_inv.at[idx, '總庫存'] = total_qty
        new_inv.at[idx, '均價'] = (total_value / total_qty) if total_qty > 0 else 0
        for w in WAREHOUSES:
            new_inv.at[idx, f'庫存_{w}'] = w_stock[w]
            
    return new_inv

def gen_batch_number(prefix="BAT"):
    return f"{prefix}-{datetime.now().strftime('%y%m%d%H%M')}"

def get_safe_view(df):
    """回傳「不含敏感欄位」的表格供一般人員檢視"""
    sensitive_cols = ['進貨總成本', '均價', '工資', '運費', '款項結清']
    safe_cols = [c for c in df.columns if c not in sensitive_cols]
    return df[safe_cols]

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

st.set_page_config(page_title=PAGE_TITLE, layout="wide", page_icon="🔐")
st.title(f"🏭 {PAGE_TITLE}")

with st.sidebar:
    st.header("部門功能導航")
    
    # 權限分流選單
    page = st.radio("選擇作業", [
        "📥 進貨庫存 (無金額)", 
        "🔨 製造生產 (工廠)", 
        "🚚 銷售出貨 (業務)", 
        "📦 商品建檔與維護",
        "💰 成本與財務管理 (加密)"  # <--- 新增的受控頁面
    ])

# ---------------------------------------------------------
# 頁面 1: 進貨 (一般員工用 - 看不到成本)
# ---------------------------------------------------------
if page == "📥 進貨庫存 (無金額)":
    st.subheader("📥 進貨點收 (僅數量)")
    st.info("請輸入進貨數量。**進貨金額請交由財務部門補登。**")
    
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
            p_note = st.text_input("備註 (廠商/採購單)")
            
            if st.button("確認進貨 (金額設為0)"):
                p_row = inv_df[inv_df['label'] == p_sel].iloc[0]
                rec = {
                    '單據類型': '進貨',
                    '單號': datetime.now().strftime('%Y%m%d%H%M%S'),
                    '日期': p_date, '系列': p_row['系列'], '分類': p_row['分類'], 
                    '品名': p_row['品名'], '貨號': p_row['貨號'], '批號': gen_batch_number("IN"),
                    '倉庫': p_wh, '數量': p_qty, 'Key單者': p_user, '備註': p_note,
                    '進貨總成本': 0 # 預設為 0，待財務補登
                }
                st.session_state['history'] = pd.concat([st.session_state['history'], pd.DataFrame([rec])], ignore_index=True)
                st.session_state['inventory'] = recalculate_inventory(st.session_state['history'], st.session_state['inventory'])
                save_data()
                st.success("進貨單已建立！請通知財務補登成本。")
                time.sleep(1)
                st.rerun()
    
    # 顯示表格 (隱藏成本)
    st.caption("最近進貨紀錄")
    df = st.session_state['history']
    if not df.empty:
        df_view = df[df['單據類型'] == '進貨'].copy()
        # 使用安全視圖 (隱藏金額)
        st.dataframe(get_safe_view(df_view), use_container_width=True)

# ---------------------------------------------------------
# 頁面 2 & 3: 製造與出貨 (一般員工用 - 看不到成本)
# ---------------------------------------------------------
elif page in ["🔨 製造生產 (工廠)", "🚚 銷售出貨 (業務)"]:
    # 邏輯與之前類似，但使用 get_safe_view 隱藏欄位
    
    if page == "🔨 製造生產 (工廠)":
        st.subheader("🔨 製造生產紀錄")
        tab1, tab2 = st.tabs(["📤 領料", "📥 完工"])
        # (此處省略部分重複代碼，僅展示核心差異：表格顯示)
        # ... 輸入表單與上一版相同 ...
        
        # 顯示時隱藏敏感欄位
        df = st.session_state['history']
        mask = df['單據類型'].astype(str).str.contains('製造')
        st.dataframe(get_safe_view(df[mask]), use_container_width=True)

    elif page == "🚚 銷售出貨 (業務)":
        st.subheader("🚚 出貨紀錄")
        # ... 輸入表單與上一版相同 ...
        
        # 顯示時隱藏敏感欄位
        df = st.session_state['history']
        mask = df['單據類型'].isin(['銷售出貨', '製造領料'])
        st.dataframe(get_safe_view(df[mask]), use_container_width=True)

# ---------------------------------------------------------
# 頁面 4: 商品建檔 (無成本)
# ---------------------------------------------------------
elif page == "📦 商品建檔與維護":
    st.subheader("📦 商品建檔")
    st.info("此處僅建立商品基本資料，初始庫存與成本請至「成本管理」頁面設定。")
    # ... 建檔表單 ...
    st.dataframe(get_safe_view(st.session_state['inventory']), use_container_width=True)

# ---------------------------------------------------------
# 頁面 5: 成本與財務管理 (加密區)
# ---------------------------------------------------------
elif page == "💰 成本與財務管理 (加密)":
    st.subheader("💰 成本與財務中心")
    
    # 密碼鎖
    pwd = st.text_input("請輸入管理員密碼", type="password")
    
    if pwd == ADMIN_PASSWORD:
        st.success("身分驗證成功")
        
        tab_fix, tab_full, tab_inv = st.tabs(["💸 補登進貨成本", "📜 完整流水帳 (含金額)", "📊 庫存資產總表"])
        
        # 1. 補登成本功能
        with tab_fix:
            st.markdown("#### 待補登成本的進貨單")
            st.caption("以下列表顯示「數量 > 0」但「成本 = 0」的進貨紀錄。請直接修改「進貨總成本」欄位。")
            
            df = st.session_state['history']
            # 篩選條件：是進貨單 且 成本為 0
            mask_fix = (df['單據類型'] == '進貨') & (df['進貨總成本'] == 0)
            df_fix = df[mask_fix].copy()
            
            if df_fix.empty:
                st.info("✅ 目前沒有需要補登成本的單據。")
            else:
                # 這裡顯示完整欄位讓老闆改
                edited_fix = st.data_editor(
                    df_fix,
                    column_config={
                        "進貨總成本": st.column_config.NumberColumn("進貨總成本 (請輸入)", required=True, format="$%d")
                    },
                    use_container_width=True
                )
                
                if st.button("💾 儲存成本補登"):
                    # 將修改後的資料寫回總表
                    # 透過 index 更新
                    df.update(edited_fix)
                    st.session_state['history'] = df
                    st.session_state['inventory'] = recalculate_inventory(df, st.session_state['inventory'])
                    save_data()
                    st.success("成本已更新！庫存均價已重新計算。")
                    time.sleep(1)
                    st.rerun()

        # 2. 完整流水帳
        with tab_full:
            st.write("此處顯示包含「工資」、「運費」、「進貨總成本」的完整紀錄。")
            edited_all = st.data_editor(st.session_state['history'], use_container_width=True, num_rows="dynamic")
            if st.button("💾 儲存總表修正"):
                st.session_state['history'] = edited_all
                st.session_state['inventory'] = recalculate_inventory(edited_all, st.session_state['inventory'])
                save_data()
                st.success("已更新")

        # 3. 庫存資產
        with tab_inv:
            st.write("此處顯示包含「平均成本」的庫存表。")
            st.dataframe(
                st.session_state['inventory'],
                use_container_width=True,
                column_config={
                    "均價": st.column_config.NumberColumn(format="$%.2f"),
                    "總庫存": st.column_config.NumberColumn(format="%d")
                }
            )

    elif pwd != "":
        st.error("密碼錯誤")
