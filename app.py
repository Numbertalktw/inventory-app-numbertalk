import streamlit as st
import pandas as pd
import sqlite3
from datetime import date, datetime, timedelta
import os
import time
import io
import re

# ==========================================
# 1. 系統設定
# ==========================================
PAGE_TITLE = "製造庫存系統 (多倉總表匯入版)"
DB_FILE = "inventory_system.db"
ADMIN_PASSWORD = "8888"

# 固定選項 (請確保 Excel 標題列的倉庫名稱與這裡完全一致)
WAREHOUSES = ["Wen", "千畇", "James", "Imeng"]
CATEGORIES = ["天然石", "金屬配件", "線材", "包裝材料", "完成品"]
SERIES = ["原料", "半成品", "成品", "包材"]
KEYERS = ["Wen", "千畇", "James", "Imeng", "小幫手"]

# 預設庫存調整原因
DEFAULT_REASONS = ["盤點差異", "報廢", "樣品借出", "系統修正", "其他"]

# ==========================================
# 2. 資料庫核心 (SQLite)
# ==========================================

def get_connection():
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    return conn

def init_db():
    conn = get_connection()
    c = conn.cursor()
    # 1. 商品主檔
    c.execute('''CREATE TABLE IF NOT EXISTS products 
                 (sku TEXT PRIMARY KEY, name TEXT, category TEXT, series TEXT, spec TEXT)''')
    # 2. 庫存表
    c.execute('''CREATE TABLE IF NOT EXISTS stock 
                 (sku TEXT, warehouse TEXT, qty REAL, PRIMARY KEY (sku, warehouse))''')
    # 3. 流水帳
    c.execute('''CREATE TABLE IF NOT EXISTS history 
                 (id INTEGER PRIMARY KEY AUTOINCREMENT, doc_type TEXT, doc_no TEXT, date TEXT, 
                  sku TEXT, warehouse TEXT, qty REAL, user TEXT, note TEXT, cost REAL, 
                  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)''')
    conn.commit()
    conn.close()

def reset_db():
    conn = get_connection()
    c = conn.cursor()
    c.execute("DROP TABLE IF EXISTS products")
    c.execute("DROP TABLE IF EXISTS stock")
    c.execute("DROP TABLE IF EXISTS history")
    conn.commit()
    conn.close()
    init_db()

# --- 資料操作函式 ---

def add_product(sku, name, category, series, spec):
    conn = get_connection()
    c = conn.cursor()
    try:
        c.execute("INSERT INTO products (sku, name, category, series, spec) VALUES (?, ?, ?, ?, ?)",
                  (sku, name, category, series, spec))
        for wh in WAREHOUSES:
            c.execute("INSERT OR IGNORE INTO stock (sku, warehouse, qty) VALUES (?, ?, 0)", (sku, wh))
        conn.commit()
        return True, "成功"
    except sqlite3.IntegrityError:
        return False, "貨號已存在，無法重複建立"
    except Exception as e:
        return False, str(e)
    finally:
        conn.close()

def get_all_products():
    conn = get_connection()
    df = pd.read_sql("SELECT * FROM products", conn)
    conn.close()
    return df

def get_current_stock(sku, warehouse):
    conn = get_connection()
    c = conn.cursor()
    c.execute("SELECT qty FROM stock WHERE sku=? AND warehouse=?", (sku, warehouse))
    res = c.fetchone()
    conn.close()
    return res[0] if res else 0.0

def get_stock_overview():
    conn = get_connection()
    df_prod = pd.read_sql("SELECT * FROM products", conn)
    df_stock = pd.read_sql("SELECT * FROM stock", conn)
    conn.close()
    
    if df_prod.empty: return pd.DataFrame()
    if df_stock.empty:
        result = df_prod.copy()
        for wh in WAREHOUSES: result[wh] = 0.0
        result['總庫存'] = 0.0
        return result

    pivot = df_stock.pivot(index='sku', columns='warehouse', values='qty').fillna(0)
    for wh in WAREHOUSES:
        if wh not in pivot.columns: pivot[wh] = 0.0
            
    pivot['總庫存'] = pivot[WAREHOUSES].sum(axis=1)
    result = pd.merge(df_prod, pivot, on='sku', how='left').fillna(0)
    
    cols = ['sku', 'series', 'category', 'name', 'spec', '總庫存'] + WAREHOUSES
    final_cols = [c for c in cols if c in result.columns]
    return result[final_cols]

def add_transaction(doc_type, date_str, sku, wh, qty, user, note, cost=0):
    conn = get_connection()
    c = conn.cursor()
    try:
        doc_prefix = {
            "進貨": "IN", "銷售出貨": "OUT", "製造領料": "MO", "製造入庫": "PD",
            "庫存調整(加)": "ADJ+", "庫存調整(減)": "ADJ-", "期初建檔": "OPEN"
        }.get(doc_type, "DOC")
        
        doc_no = f"{doc_prefix}-{int(time.time())}"
        
        c.execute('''INSERT INTO history (doc_type, doc_no, date, sku, warehouse, qty, user, note, cost)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)''', 
                  (doc_type, doc_no, date_str, sku, wh, qty, user, note, cost))
        
        factor = 1
        if doc_type in ['銷售出貨', '製造領料', '庫存調整(減)']:
            factor = -1
        
        change_qty = qty * factor
        
        c.execute('''INSERT INTO stock (sku, warehouse, qty) VALUES (?, ?, ?)
                     ON CONFLICT(sku, warehouse) DO UPDATE SET qty = qty + ?''', 
                  (sku, wh, change_qty, change_qty))
        
        conn.commit()
        return True
    except Exception as e:
        return False
    finally:
        conn.close()

def get_distinct_reasons():
    conn = get_connection()
    query = """
    SELECT DISTINCT note FROM history 
    WHERE doc_type LIKE '庫存調整%' AND note NOT LIKE '%批量%' AND note NOT LIKE '%修正%'
    ORDER BY note
    """
    try:
        df = pd.read_sql(query, conn)
        return sorted(list(set(DEFAULT_REASONS + df['note'].tolist())))
    except:
        return DEFAULT_REASONS
    finally:
        conn.close()

# --- ★ 修正核心：支援不分大小寫讀取 SKU ---
def process_full_stock_import(file_obj):
    """
    讀取包含多個倉庫欄位的總表，並自動比對差異進行更新
    """
    try:
        df = pd.read_csv(file_obj) if file_obj.name.endswith('.csv') else pd.read_excel(file_obj)
        df.columns = [str(c).strip() for c in df.columns]
        
        # 1. 識別欄位 (轉為統一標準名稱，不分大小寫)
        rename_map = {}
        for c in df.columns:
            c_upper = c.upper()
            if c_upper in ['SKU', '編號', '料號']: rename_map[c] = '貨號'
        
        df = df.rename(columns=rename_map)
        
        if '貨號' not in df.columns:
            return False, f"錯誤：Excel 必須包含 `貨號` 或 `SKU` 欄位。讀取到的欄位：{list(df.columns)}"

        # 2. 找出檔案中存在的倉庫欄位 (交集)
        target_warehouses = [wh for wh in WAREHOUSES if wh in df.columns]
        
        if not target_warehouses:
            return False, f"錯誤：找不到任何倉庫欄位。請確認 Excel 標題包含: {', '.join(WAREHOUSES)}"

        update_count = 0
        skip_count = 0

        # 3. 逐行、逐倉比對
        for _, row in df.iterrows():
            sku = str(row['貨號']).strip()
            if not sku or sku.lower() == 'nan': continue
            
            for wh in target_warehouses:
                try:
                    # 讀取 Excel 中的數量 (空白視為 0)
                    val = row[wh]
                    new_qty = float(val) if pd.notna(val) and str(val).strip() != '' else 0.0
                except:
                    continue # 略過非數字

                current_qty = get_current_stock(sku, wh)
                diff = new_qty - current_qty
                
                if abs(diff) > 0.0001: # 有差異才更新
                    if current_qty == 0 and diff > 0:
                        doc_type = "期初建檔"
                        note = "總表匯入-期初"
                    else:
                        doc_type = "庫存調整(加)" if diff > 0 else "庫存調整(減)"
                        note = f"總表盤點修正 ({wh})"
                    
                    add_transaction(doc_type, str(date.today()), sku, wh, abs(diff), "系統匯入", note)
                    update_count += 1
                else:
                    skip_count += 1
        
        return True, f"✅ 匯入成功！共掃描 {len(target_warehouses)} 個倉庫欄位。\n已更新 {update_count} 筆異動，{skip_count} 筆無變動。"
        
    except Exception as e:
        return False, str(e)

def get_history(doc_type_filter=None, start_date=None, end_date=None):
    conn = get_connection()
    query = """
    SELECT h.date as '日期', h.doc_type as '單據類型', h.doc_no as '單號',
           p.series as '系列', p.category as '分類', p.name as '品名', p.spec as '規格',
           h.sku as '貨號', h.warehouse as '倉庫', h.qty as '數量', 
           h.user as '經手人', h.note as '備註'
    FROM history h LEFT JOIN products p ON h.sku = p.sku WHERE 1=1
    """
    params = []
    if doc_type_filter:
        if isinstance(doc_type_filter, list):
            placeholders = ','.join(['?'] * len(doc_type_filter))
            query += f" AND h.doc_type IN ({placeholders})"
            params.extend(doc_type_filter)
        else:
            query += " AND h.doc_type LIKE ?"
            params.append(f"%{doc_type_filter}%")
    if start_date and end_date:
        query += " AND h.date BETWEEN ? AND ?"
        params.extend([str(start_date), str(end_date)])
    query += " ORDER BY h.id DESC LIMIT 50"
    try: df = pd.read_sql(query, conn, params=params)
    except: df = pd.DataFrame()
    conn.close()
    return df

def get_period_summary(start_date, end_date):
    conn = get_connection()
    query = """
    SELECT h.sku, h.doc_type, SUM(h.qty) as total_qty FROM history h
    WHERE h.date BETWEEN ? AND ? GROUP BY h.sku, h.doc_type
    """
    try:
        df_raw = pd.read_sql(query, conn, params=(str(start_date), str(end_date)))
        if df_raw.empty: return pd.DataFrame()
        pivot = df_raw.pivot(index='sku', columns='doc_type', values='total_qty').fillna(0)
        for col in ['進貨', '銷售出貨', '製造入庫', '製造領料']:
            if col not in pivot.columns: pivot[col] = 0.0
        df_prod = pd.read_sql("SELECT sku, name, category, spec FROM products", conn)
        result = pd.merge(df_prod, pivot, on='sku', how='inner')
        result = result.rename(columns={'sku': '貨號', 'name': '品名', 'category': '分類', 'spec': '規格', '進貨': '期間進貨量', '銷售出貨': '期間出貨量', '製造入庫': '期間生產量', '製造領料': '期間領料量'})
        cols = ['貨號', '分類', '品名', '規格', '期間進貨量', '期間出貨量', '期間生產量', '期間領料量']
        return result[[c for c in cols if c in result.columns]]
    except: return pd.DataFrame()
    finally: conn.close()

def to_excel_download(df):
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False)
    return output.getvalue()

# ==========================================
# 3. 初始化
# ==========================================
st.set_page_config(page_title=PAGE_TITLE, layout="wide", page_icon="🏭")
init_db()

# ==========================================
# 4. 介面邏輯
# ==========================================
st.title(f"🏭 {PAGE_TITLE}")

with st.sidebar:
    st.header("功能選單")
    page = st.radio("前往", ["📦 商品管理 (建檔/匯入)", "📥 進貨作業", "🚚 出貨作業", "🔨 製造作業", "⚖️ 庫存盤點", "📊 報表查詢"])
    st.divider()
    if st.button("🔴 初始化/重置資料庫"):
        reset_db(); st.cache_data.clear(); st.success("已重置！"); time.sleep(1); st.rerun()

# ------------------------------------------------------------------
# 1. 商品管理
# ------------------------------------------------------------------
if page == "📦 商品管理 (建檔/匯入)":
    st.subheader("📦 商品資料維護")
    tab1, tab2, tab3 = st.tabs(["✨ 單筆建檔", "📂 匯入商品資料", "📥 匯入期初完整總表"])
    
    with tab1:
        with st.form("add_prod"):
            c1, c2 = st.columns(2)
            sku = c1.text_input("貨號 (SKU) *必填")
            name = c2.text_input("品名 *必填")
            c3, c4, c5 = st.columns(3)
            cat = c3.selectbox("分類", CATEGORIES)
            ser = c4.selectbox("系列", SERIES)
            spec = c5.text_input("規格/尺寸")
            if st.form_submit_button("新增商品"):
                if sku and name:
                    success, msg = add_product(sku, name, cat, ser, spec)
                    if success: st.success(f"成功"); time.sleep(1); st.rerun()
                    else: st.error(msg)
                else: st.error("必填欄位缺漏")

    with tab2:
        st.info("請上傳 Excel (欄位：`貨號`, `品名`, `分類`, `系列`, `規格`)")
        up = st.file_uploader("上傳商品清單", type=['xlsx', 'csv'], key='prod_up')
        if up and st.button("開始匯入商品"):
            try:
                df = pd.read_csv(up) if up.name.endswith('.csv') else pd.read_excel(up)
                df.columns = [str(c).strip() for c in df.columns]
                
                # ★ 修正商品匯入邏輯 (Case-Insensitive)
                rename_map = {}
                for c in df.columns:
                    c_up = c.upper()
                    if c_up in ['SKU', '編號', '料號']: rename_map[c] = '貨號'
                    if c_up in ['名稱', '商品名稱', 'NAME']: rename_map[c] = '品名'
                    if c_up in ['類別', 'CATEGORY']: rename_map[c] = '分類'
                    if c_up in ['SERIES']: rename_map[c] = '系列'
                    if c_up in ['尺寸', 'SPEC']: rename_map[c] = '規格'
                df = df.rename(columns=rename_map)
                
                count = 0
                if '貨號' in df.columns:
                    for _, row in df.iterrows():
                        s = str(row.get('貨號', '')).strip()
                        n = str(row.get('品名', '')).strip()
                        if s and s.lower() != 'nan':
                            # 若有貨號但無品名，可以選擇略過或允許空白(更新庫存用)
                            # 這裡為了安全，若為新商品建議要有品名
                            add_product(s, n, str(row.get('分類', '未分類')), str(row.get('系列', '未分類')), str(row.get('規格', '')))
                            count += 1
                    st.success(f"已掃描並匯入 {count} 筆資料")
                    time.sleep(1); st.rerun()
                else: st.error("Excel 缺少 `貨號` 欄位")
            except Exception as e: st.error(f"匯入失敗: {e}")

    with tab3:
        st.markdown("### 📥 匯入期初完整總表")
        st.info(f"此功能支援一次匯入多個倉庫的數量。請上傳包含 `貨號` (或 `sku`) 以及倉庫名稱 ({', '.join(WAREHOUSES)}) 的 Excel 檔。")
        up_stock = st.file_uploader("上傳完整庫存總表", type=['xlsx', 'csv'], key='stock_up_full_init')
        if up_stock and st.button("開始匯入期初庫存"):
            success, msg = process_full_stock_import(up_stock)
            if success: st.success(msg); time.sleep(3); st.rerun()
            else: st.error(msg)

    st.divider()
    st.dataframe(get_all_products(), use_container_width=True)

# ------------------------------------------------------------------
# 2. 進貨 / 3. 出貨 / 4. 製造 (保持不變)
# ------------------------------------------------------------------
elif page == "📥 進貨作業":
    st.subheader("📥 進貨入庫")
    prods = get_all_products()
    if not prods.empty:
        prods['label'] = prods['sku'] + " | " + prods['name']
        with st.form("in"):
            c1, c2 = st.columns([2, 1])
            sel_prod = c1.selectbox("商品", prods['label'])
            wh = c2.selectbox("倉庫", WAREHOUSES)
            c3, c4 = st.columns(2)
            qty = c3.number_input("數量", 1)
            d_val = c4.date_input("日期", date.today())
            user = st.selectbox("經手人", KEYERS)
            note = st.text_input("備註")
            if st.form_submit_button("進貨"):
                if add_transaction("進貨", str(d_val), sel_prod.split(" | ")[0], wh, qty, user, note):
                    st.success("成功"); time.sleep(0.5); st.rerun()
        st.dataframe(get_history("進貨"), use_container_width=True)

elif page == "🚚 出貨作業":
    st.subheader("🚚 銷售出貨")
    prods = get_all_products()
    if not prods.empty:
        prods['label'] = prods['sku'] + " | " + prods['name']
        with st.form("out"):
            c1, c2 = st.columns([2, 1])
            sel_prod = c1.selectbox("商品", prods['label'])
            wh = c2.selectbox("倉庫", WAREHOUSES, index=2)
            c3, c4 = st.columns(2)
            qty = c3.number_input("數量", 1)
            d_val = c4.date_input("日期", date.today())
            user = st.selectbox("經手人", KEYERS)
            note = st.text_input("訂單/備註")
            if st.form_submit_button("出貨"):
                if add_transaction("銷售出貨", str(d_val), sel_prod.split(" | ")[0], wh, qty, user, note):
                    st.success("成功"); time.sleep(0.5); st.rerun()
        st.dataframe(get_history("銷售出貨"), use_container_width=True)

elif page == "🔨 製造作業":
    st.subheader("🔨 生產管理")
    prods = get_all_products()
    if not prods.empty:
        prods['label'] = prods['sku'] + " | " + prods['name']
        t1, t2 = st.tabs(["領料", "完工"])
        with t1:
            with st.form("mo1"):
                sel = st.selectbox("原料", prods['label'])
                wh = st.selectbox("倉庫", WAREHOUSES)
                qty = st.number_input("量", 1)
                if st.form_submit_button("領料"):
                    add_transaction("製造領料", str(date.today()), sel.split(" | ")[0], wh, qty, "工廠", "領料")
                    st.success("OK"); time.sleep(0.5); st.rerun()
        with t2:
            with st.form("mo2"):
                sel = st.selectbox("成品", prods['label'])
                wh = st.selectbox("倉庫", WAREHOUSES)
                qty = st.number_input("量", 1)
                if st.form_submit_button("完工"):
                    add_transaction("製造入庫", str(date.today()), sel.split(" | ")[0], wh, qty, "工廠", "完工")
                    st.success("OK"); time.sleep(0.5); st.rerun()
        st.dataframe(get_history(["製造領料", "製造入庫"]), use_container_width=True)

# ------------------------------------------------------------------
# 5. 庫存盤點 (更新)
# ------------------------------------------------------------------
elif page == "⚖️ 庫存盤點":
    st.subheader("⚖️ 庫存調整")
    t1, t2 = st.tabs(["👋 單筆調整", "📂 匯入完整庫存總表"])
    prods = get_all_products()
    
    with t1:
        if not prods.empty:
            prods['label'] = prods['sku'] + " | " + prods['name']
            reason_options = get_distinct_reasons() + ["➕ 手動輸入"]
            with st.form("adj"):
                c1, c2 = st.columns(2)
                sel = c1.selectbox("商品", prods['label'])
                wh = c2.selectbox("倉庫", WAREHOUSES)
                c3, c4 = st.columns(2)
                act = c3.radio("動作", ["增加 (+)", "減少 (-)"], horizontal=True)
                qty = c4.number_input("量", 1)
                res = st.selectbox("原因", reason_options)
                if res == "➕ 手動輸入": res = st.text_input("輸入原因")
                if st.form_submit_button("調整"):
                    tp = "庫存調整(加)" if act == "增加 (+)" else "庫存調整(減)"
                    add_transaction(tp, str(date.today()), sel.split(" | ")[0], wh, qty, "管理員", res)
                    st.success("OK"); time.sleep(0.5); st.rerun()

    with t2:
        st.markdown("### 📥 上傳完整庫存總表 (盤點修正)")
        st.info(f"此功能會讀取 Excel 中對應倉庫名稱的欄位 ({', '.join(WAREHOUSES)})，並自動計算差異進行多倉修正。")
        
        up_stock = st.file_uploader("上傳完整盤點表", type=['xlsx', 'csv'], key='stock_up_full_adj')
        if up_stock and st.button("開始比對並更新庫存"):
            success, msg = process_full_stock_import(up_stock)
            if success: 
                st.success(msg)
                time.sleep(3)
                st.rerun()
            else: 
                st.error(msg)
    
    st.divider()
    st.dataframe(get_stock_overview(), use_container_width=True)

# ------------------------------------------------------------------
# 6. 報表查詢
# ------------------------------------------------------------------
elif page == "📊 報表查詢":
    st.subheader("📊 數據報表中心")
    t1, t2, t3 = st.tabs(["📦 庫存總表", "📅 期間統計", "📜 流水帳"])
    
    with t1:
        df = get_stock_overview()
        st.dataframe(df, use_container_width=True)
        if not df.empty:
            st.download_button("📥 下載完整總表", to_excel_download(df), f"Stock_All_{date.today()}.xlsx")
            st.divider()
            st.markdown("#### 🏢 分倉庫存下載")
            cols = st.columns(len(WAREHOUSES))
            for i, wh in enumerate(WAREHOUSES):
                # 只下載該倉庫有數據的欄位
                target_cols = ['sku', 'series', 'category', 'name', 'spec', wh]
                df_wh = df[[c for c in target_cols if c in df.columns]].copy()
                with cols[i]:
                    st.download_button(f"📥 {wh} 庫存", to_excel_download(df_wh), f"Stock_{wh}.xlsx")

    with t2:
        c1, c2 = st.columns(2)
        d1 = c1.date_input("起", date.today().replace(day=1))
        d2 = c2.date_input("迄", date.today())
        if st.button("生成"):
            df_p = get_period_summary(d1, d2)
            if not df_p.empty:
                st.dataframe(df_p, use_container_width=True)
                st.download_button("下載", to_excel_download(df_p), "Report.xlsx")
            else: st.info("無資料")

    with t3:
        st.markdown("#### 下載明細")
        c1, c2, c3, c4 = st.columns(4)
        c1.download_button("進貨明細", to_excel_download(get_history("進貨")), "In.xlsx")
        c2.download_button("出貨明細", to_excel_download(get_history("銷售出貨")), "Out.xlsx")
        c3.download_button("製造明細", to_excel_download(get_history(["製造領料", "製造入庫"])), "Mfg.xlsx")
        c4.download_button("完整流水帳", to_excel_download(get_history()), "All_Logs.xlsx")
