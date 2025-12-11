import streamlit as st
import pandas as pd
from datetime import date, datetime
import os
import time

# ==========================================
# 1. 核心設定與欄位定義
# ==========================================

# 系統設定
PAGE_TITLE = "商品庫存管理系統 (16欄位版)"
INVENTORY_FILE = 'inventory_simple_v1.csv'
HISTORY_FILE = 'history_16cols_v1.csv'

# --- 核心重點：您指定的 16 個欄位順序 ---
# 注意：為了程式處理方便，部分欄位名稱微調 (例如移除括號說明)，但在顯示時會設定標題
HISTORY_COLUMNS = [
    '該紀錄的單號', 
    '日期', 
    '商品系列', 
    '商品分類', 
    '商品品名', 
    '商品貨號', 
    '出庫單號',      # (可複寫)
    '出庫_入庫',     # (下拉式選單)
    '數量', 
    '經手人', 
    '訂單單號', 
    '出貨日期', 
    '出貨單據號碼', 
    '工資', 
    '訂單發票號碼', 
    '備註'
]

# 庫存檔 (Inventory) 只需要保留商品的基本資料與當前庫存量
# 這是為了讓系統知道現在有哪些商品可以選
INVENTORY_COLUMNS = [
    '商品貨號', '商品系列', '商品分類', '商品品名', 
    '庫存數量', '平均成本'
]

# 預設選單資料
DEFAULT_SERIES = ["一般款", "高定款", "限量款", "福利品", "客製化"]
DEFAULT_CATEGORIES = ["天然石", "配件", "耗材", "包材", "成品"]
DEFAULT_HANDLERS = ["店長", "小幫手A", "小幫手B", "行政"]

# ==========================================
# 2. 資料處理函式
# ==========================================

def load_data():
    """讀取資料，若無檔案則建立空 DataFrame"""
    # 1. 庫存
    if os.path.exists(INVENTORY_FILE):
        try:
            inv_df = pd.read_csv(INVENTORY_FILE)
            # 確保欄位正確
            for col in INVENTORY_COLUMNS:
                if col not in inv_df.columns:
                    inv_df[col] = 0 if '數量' in col or '成本' in col else ""
            inv_df['商品貨號'] = inv_df['商品貨號'].astype(str)
        except:
            inv_df = pd.DataFrame(columns=INVENTORY_COLUMNS)
    else:
        inv_df = pd.DataFrame(columns=INVENTORY_COLUMNS)

    # 2. 歷史紀錄
    if os.path.exists(HISTORY_FILE):
        try:
            hist_df = pd.read_csv(HISTORY_FILE)
            # 補齊欄位
            for col in HISTORY_COLUMNS:
                if col not in hist_df.columns:
                    hist_df[col] = ""
            # 確保順序
            hist_df = hist_df[HISTORY_COLUMNS]
        except:
            hist_df = pd.DataFrame(columns=HISTORY_COLUMNS)
    else:
        hist_df = pd.DataFrame(columns=HISTORY_COLUMNS)
        
    return inv_df, hist_df

def save_data():
    """儲存資料到 CSV"""
    if 'inventory' in st.session_state:
        st.session_state['inventory'].to_csv(INVENTORY_FILE, index=False, encoding='utf-8-sig')
    if 'history' in st.session_state:
        st.session_state['history'].to_csv(HISTORY_FILE, index=False, encoding='utf-8-sig')

def generate_sku(category, df):
    """簡單的貨號生成器"""
    prefix_map = {'天然石': 'ST', '配件': 'AC', '耗材': 'OT', '包材': 'PK', '成品': 'PD'}
    prefix = prefix_map.get(category, "XX")
    
    if df.empty: return f"{prefix}0001"
    
    # 篩選同開頭的貨號
    mask = df['商品貨號'].astype(str).str.startswith(prefix)
    existing = df.loc[mask, '商品貨號']
    
    if existing.empty:
        return f"{prefix}0001"
    
    try:
        # 取出數字部分找最大值
        max_num = existing.str.extract(r'(\d+)')[0].astype(float).max()
        return f"{prefix}{int(max_num)+1:04d}"
    except:
        return f"{prefix}{int(time.time())}"

def get_options(df, col, default):
    """取得下拉選單 (合併現有資料)"""
    opts = set(default)
    if not df.empty and col in df.columns:
        exist = df[col].dropna().unique().tolist()
        opts.update([str(x) for x in exist if str(x).strip()])
    return ["➕ 手動輸入"] + sorted(list(opts))

# ==========================================
# 3. 初始化 Session State
# ==========================================

if 'inventory' not in st.session_state:
    inv_data, hist_data = load_data()
    st.session_state['inventory'] = inv_data
    st.session_state['history'] = hist_data

# ==========================================
# 4. Streamlit UI
# ==========================================

st.set_page_config(page_title=PAGE_TITLE, layout="wide", page_icon="📋")
st.title(f"📋 {PAGE_TITLE}")

# 側邊欄導航
with st.sidebar:
    st.header("功能導航")
    page = st.radio("前往", ["📝 庫存異動 (入庫/出庫)", "📦 商品建檔與庫存表", "📜 歷史紀錄查詢 (16欄位)"])
    
    st.divider()
    st.write("📥 資料備份")
    if not st.session_state['history'].empty:
        csv_h = st.session_state['history'].to_csv(index=False).encode('utf-8-sig')
        st.download_button("下載紀錄總表 (CSV)", csv_h, f'History_{date.today()}.csv', "text/csv")
    
    if not st.session_state['inventory'].empty:
        csv_i = st.session_state['inventory'].to_csv(index=False).encode('utf-8-sig')
        st.download_button("下載庫存清單 (CSV)", csv_i, f'Inventory_{date.today()}.csv', "text/csv")

# ---------------------------------------------------------
# 頁面 1: 庫存異動 (核心操作區)
# ---------------------------------------------------------
if page == "📝 庫存異動 (入庫/出庫)":
    st.subheader("📝 新增異動紀錄")
    st.info("在此輸入每一筆「進貨」或「出貨」資料，系統會自動更新庫存並寫入紀錄表。")

    inv_df = st.session_state['inventory']
    
    if inv_df.empty:
        st.warning("⚠️ 目前無商品資料，請先前往「商品建檔與庫存表」建立商品。")
    else:
        # --- 步驟 1: 選擇商品 ---
        # 製作顯示標籤
        inv_df['label'] = inv_df['商品貨號'] + " | " + inv_df['商品品名'] + " | 庫存:" + inv_df['庫存數量'].astype(str)
        
        c1, c2 = st.columns([2, 1])
        with c1:
            selected_label = st.selectbox("🔍 選擇商品", inv_df['label'].tolist())
            # 找到對應的 row
            target_row = inv_df[inv_df['label'] == selected_label].iloc[0]
            target_idx = inv_df[inv_df['label'] == selected_label].index[0]

        with c2:
            action_type = st.radio("動作類型", ["入庫 (進貨/退貨入庫)", "出庫 (銷售/耗損)"], horizontal=True)

        st.divider()

        # --- 步驟 2: 填寫 16 欄位所需的資料 ---
        with st.form("transaction_form"):
            st.markdown("##### 📦 異動明細")
            
            # 自動帶入的欄位 (唯讀)
            col_info1, col_info2, col_info3, col_info4 = st.columns(4)
            col_info1.text_input("商品系列", value=target_row['商品系列'], disabled=True)
            col_info2.text_input("商品分類", value=target_row['商品分類'], disabled=True)
            col_info3.text_input("商品品名", value=target_row['商品品名'], disabled=True)
            col_info4.text_input("商品貨號", value=target_row['商品貨號'], disabled=True)
            
            # 第一行：核心數據
            r1_c1, r1_c2, r1_c3, r1_c4 = st.columns(4)
            txn_date = r1_c1.date_input("日期", value=date.today())
            qty = r1_c2.number_input("數量", min_value=1, value=1)
            handler = r1_c3.selectbox("經手人", DEFAULT_HANDLERS)
            cost_input = r1_c4.number_input("本次總成本/進貨價 (入庫用)", min_value=0, value=0, help="出庫時通常不填，僅入庫計算成本用")

            st.markdown("##### 🚚 單據資訊 (出庫/訂單必填)")
            # 第二行：單據相關
            r2_c1, r2_c2, r2_c3, r2_c4 = st.columns(4)
            order_id = r2_c1.text_input("訂單單號", placeholder="例如：蝦皮單號")
            ship_date = r2_c2.date_input("出貨日期", value=date.today())
            doc_id = r2_c3.text_input("出貨單據號碼", placeholder="物流單號")
            invoice_id = r2_c4.text_input("訂單發票號碼")

            # 第三行：其他
            r3_c1, r3_c2, r3_c3 = st.columns([1, 1, 2])
            labor_cost = r3_c1.number_input("工資", min_value=0, value=0)
            out_id_custom = r3_c2.text_input("出庫單號 (選填)", placeholder="留空自動產生")
            note = r3_c3.text_input("備註")

            submitted = st.form_submit_button("✅ 確認送出", type="primary")

            if submitted:
                # 1. 準備資料
                now_str = datetime.now().strftime('%Y%m%d-%H%M%S')
                record_id = f"REC-{now_str}" # 該紀錄的單號
                
                final_action = "入庫" if "入庫" in action_type else "出庫"
                
                # 若是出庫，且使用者沒填出庫單號，則自動產生一個
                final_out_id = out_id_custom
                if final_action == "出庫" and not final_out_id:
                    final_out_id = f"OUT-{now_str}"

                # 2. 更新庫存邏輯
                current_qty = float(target_row['庫存數量'])
                current_cost = float(target_row['平均成本'])
                
                if final_action == "入庫":
                    new_qty = current_qty + qty
                    # 移動平均成本法
                    total_val = (current_qty * current_cost) + cost_input
                    new_avg_cost = total_val / new_qty if new_qty > 0 else 0
                    st.session_state['inventory'].at[target_idx, '庫存數量'] = new_qty
                    st.session_state['inventory'].at[target_idx, '平均成本'] = new_avg_cost
                    st.success(f"已入庫 {qty} 個，新庫存: {new_qty}")
                    
                else: # 出庫
                    new_qty = current_qty - qty
                    st.session_state['inventory'].at[target_idx, '庫存數量'] = new_qty
                    st.success(f"已出庫 {qty} 個，新庫存: {new_qty}")

                # 3. 建立 16 欄位紀錄
                new_record = {
                    '該紀錄的單號': record_id,
                    '日期': txn_date,
                    '商品系列': target_row['商品系列'],
                    '商品分類': target_row['商品分類'],
                    '商品品名': target_row['商品品名'],
                    '商品貨號': target_row['商品貨號'],
                    '出庫單號': final_out_id,
                    '出庫_入庫': final_action,
                    '數量': qty,
                    '經手人': handler,
                    '訂單單號': order_id,
                    '出貨日期': ship_date if final_action == '出庫' else None,
                    '出貨單據號碼': doc_id,
                    '工資': labor_cost,
                    '訂單發票號碼': invoice_id,
                    '備註': note
                }
                
                # 寫入 DataFrame
                st.session_state['history'] = pd.concat(
                    [st.session_state['history'], pd.DataFrame([new_record])], 
                    ignore_index=True
                )
                
                # 存檔
                save_data()
                time.sleep(1)
                st.rerun()

# ---------------------------------------------------------
# 頁面 2: 商品建檔與庫存表
# ---------------------------------------------------------
elif page == "📦 商品建檔與庫存表":
    st.subheader("📦 商品資料庫管理")
    
    tab_new, tab_list = st.tabs(["✨ 建立新商品", "📋 現有庫存清單"])
    
    with tab_new:
        st.write("若有新開發的商品，請先在此建檔。")
        with st.form("create_item"):
            c1, c2 = st.columns(2)
            cat_opts = get_options(st.session_state['inventory'], '商品分類', DEFAULT_CATEGORIES)
            cat_sel = c1.selectbox("商品分類", cat_opts)
            final_cat = c1.text_input("輸入新分類") if cat_sel == "➕ 手動輸入" else cat_sel
            
            ser_opts = get_options(st.session_state['inventory'], '商品系列', DEFAULT_SERIES)
            ser_sel = c2.selectbox("商品系列", ser_opts)
            final_ser = c2.text_input("輸入新系列") if ser_sel == "➕ 手動輸入" else ser_sel
            
            name = st.text_input("商品品名", placeholder="例如：冰種黑曜石")
            
            # 預先計算貨號
            auto_sku = generate_sku(final_cat, st.session_state['inventory'])
            sku = st.text_input("商品貨號 (預設自動產生)", value=auto_sku)
            
            st.markdown("---")
            st.caption("初始庫存設定 (可填 0)")
            cc1, cc2 = st.columns(2)
            init_qty = cc1.number_input("初始數量", min_value=0, value=0)
            init_cost = cc2.number_input("單顆成本", min_value=0.0, value=0.0)
            
            if st.form_submit_button("建立資料"):
                if not name:
                    st.error("品名為必填")
                else:
                    new_row = {
                        '商品貨號': sku,
                        '商品系列': final_ser,
                        '商品分類': final_cat,
                        '商品品名': name,
                        '庫存數量': init_qty,
                        '平均成本': init_cost
                    }
                    st.session_state['inventory'] = pd.concat(
                        [st.session_state['inventory'], pd.DataFrame([new_row])], 
                        ignore_index=True
                    )
                    
                    # 若有初始數量，也寫入一筆紀錄
                    if init_qty > 0:
                        log = {
                            '該紀錄的單號': f"INIT-{sku}",
                            '日期': date.today(),
                            '商品系列': final_ser, '商品分類': final_cat, '商品品名': name, '商品貨號': sku,
                            '出庫單號': '', '出庫_入庫': '入庫',
                            '數量': init_qty, '經手人': '系統', '訂單單號': '初始建檔',
                            '出貨日期': None, '出貨單據號碼': '', '工資': 0, '訂單發票號碼': '', '備註': '新品建檔初始庫存'
                        }
                        st.session_state['history'] = pd.concat([st.session_state['history'], pd.DataFrame([log])], ignore_index=True)
                    
                    save_data()
                    st.success(f"成功建立：{name}")
                    st.rerun()

    with tab_list:
        st.write("目前所有商品的庫存狀況：")
        st.dataframe(
            st.session_state['inventory'], 
            use_container_width=True,
            column_config={
                "平均成本": st.column_config.NumberColumn(format="$%.2f"),
                "庫存數量": st.column_config.NumberColumn(format="%d")
            }
        )

# ---------------------------------------------------------
# 頁面 3: 歷史紀錄查詢 (16欄位)
# ---------------------------------------------------------
elif page == "📜 歷史紀錄查詢 (16欄位)":
    st.subheader("📜 歷史紀錄總表")
    st.caption("這是依照您要求的 16 個欄位顯示的紀錄表。您可以在此直接修改「出庫單號」、「備註」等欄位。")
    
    df_hist = st.session_state['history']
    
    # 搜尋功能
    search_term = st.text_input("🔍 搜尋 (訂單號/品名/貨號/備註)", "")
    if search_term:
        mask = df_hist.astype(str).apply(lambda x: x.str.contains(search_term, case=False)).any(axis=1)
        df_hist = df_hist[mask]
    
    # 設定 DataEditor 的欄位屬性
    column_config = {
        "該紀錄的單號": st.column_config.TextColumn(disabled=True, help="系統自動產生，不可改"),
        "日期": st.column_config.DateColumn(format="YYYY-MM-DD"),
        "出庫_入庫": st.column_config.SelectboxColumn(
            "出庫/入庫",
            options=["入庫", "出庫", "盤點調整", "退貨"],
            required=True,
            width="medium"
        ),
        "商品貨號": st.column_config.TextColumn(disabled=True),
        "商品品名": st.column_config.TextColumn(disabled=True),
        "數量": st.column_config.NumberColumn(format="%d"),
        "工資": st.column_config.NumberColumn(format="$%d"),
        "出庫單號": st.column_config.TextColumn(help="可複寫"),
        "備註": st.column_config.TextColumn(width="large")
    }
    
    # 顯示編輯器
    edited_df = st.data_editor(
        df_hist,
        column_config=column_config,
        use_container_width=True,
        num_rows="dynamic",
        height=600,
        key="history_editor"
    )
    
    # 儲存按鈕
    if st.button("💾 儲存修改"):
        # 這裡不自動回寫庫存數量，因為歷史紀錄的修改通常是補資料，
        # 若修改了數量欄位會導致庫存對不上。
        # 建議：僅允許修改備註、單號等文字資訊。
        st.session_state['history'] = edited_df
        save_data()
        st.success("紀錄已更新！")
