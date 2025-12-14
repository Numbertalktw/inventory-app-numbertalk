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
PAGE_TITLE = "製造庫存系統 (DB專業版)"
DB_FILE = "inventory_system.db"
ADMIN_PASSWORD = "8888"

# 固定選項
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
    """建立資料庫連線"""
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    return conn

def init_db():
    """初始化資料庫表格"""
    conn = get_connection()
    c = conn.cursor()
    
    # 1. 商品主檔
    c.execute('''
        CREATE TABLE IF NOT EXISTS products (
            sku TEXT PRIMARY KEY,
            name TEXT,
            category TEXT,
            series TEXT,
            spec TEXT
        )
    ''')
    
    # 2. 庫存表
    c.execute('''
        CREATE TABLE IF NOT EXISTS stock (
            sku TEXT,
            warehouse TEXT,
            qty REAL,
            PRIMARY KEY (sku, warehouse)
        )
    ''')
    
    # 3. 流水帳
    c.execute('''
        CREATE TABLE IF NOT EXISTS history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            doc_type TEXT,
            doc_no TEXT,
            date TEXT,
            sku TEXT,
            warehouse TEXT,
            qty REAL,
            user TEXT,
            note TEXT,
            cost REAL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    conn.commit()
    conn.close()

def reset_db():
    """強制重置資料庫"""
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
        
        c.execute('''
            INSERT INTO history (doc_type, doc_no, date, sku, warehouse, qty, user, note, cost)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (doc_type, doc_no, date_str, sku, wh, qty, user, note, cost))
        
        factor = 1
        if doc_type in ['銷售出貨', '製造領料', '庫存調整(減)']:
            factor = -1
        
        change_qty = qty * factor
        
        c.execute('''
            INSERT INTO stock (sku, warehouse, qty) VALUES (?, ?, ?)
            ON CONFLICT(sku, warehouse) DO UPDATE SET qty = qty + ?
        ''', (sku, wh, change_qty, change_qty))
        
        conn.commit()
        return True
    except Exception as e:
        st.error(f"交易失敗: {e}")
        return False
    finally:
        conn.close()

def get_distinct_reasons():
    """
    [修改] 排除自動產生的批量匯入紀錄
    """
    conn = get_connection()
    # 過濾掉包含 '批量' 或 '修正' 的原因
    query = """
    SELECT DISTINCT note 
    FROM history 
    WHERE doc_type LIKE '庫存調整%' 
    AND note IS NOT NULL 
    AND note != ''
    AND note NOT LIKE '%批量%'
    AND note NOT LIKE '%修正%'
    ORDER BY note
    """
    try:
        df = pd.read_sql(query, conn)
        historical_reasons = df['note'].tolist()
        all_reasons = sorted(list(set(DEFAULT_REASONS + historical_reasons)))
        return all_reasons
    except:
        return DEFAULT_REASONS
    finally:
        conn.close()

def process_batch_stock_update(file_obj, default_wh):
    try:
        df = pd.read_csv(file_obj) if file_obj.name.endswith('.csv') else pd.read_excel(file_obj)
        df.columns = [str(c).strip() for c in df.columns]
        rename_map = {}
        for c in df.columns:
            if c in ['SKU', '編號', '料號']: rename_map[c] = '貨號'
            if c in ['數量', '盤點數量', '實際數量', 'Qty', '庫存', '現有庫存']: rename_map[c] = '數量'
            if c in ['倉庫', 'Warehouse']: rename_map[c] = '倉庫'
        df = df.rename(columns=rename_map)
        
        if '貨號' not in df.columns or '數量' not in df.columns:
            return False, "Excel 必須包含 `貨號` 與 `數量` 欄位"

        update_count = 0
        skip_count = 0
        for _, row in df.iterrows():
            sku = str(row['貨號']).strip()
            if not sku: continue
            try: new_qty = float(row['數量'])
            except: continue 
            
            target_wh = default_wh
            if '倉庫' in df.columns and pd.notna(row['倉庫']):
                w_str = str(row['倉庫']).strip()
                if w_str in WAREHOUSES: target_wh = w_str
            
            current_qty = get_current_stock(sku, target_wh)
            diff = new_qty - current_qty
            
            if diff != 0:
                if current_qty == 0 and diff > 0:
                    doc_type = "期初建檔"
                    note = "期初庫存匯入"
                else:
                    doc_type = "庫存調整(加)" if diff > 0 else "庫存調整(減)"
                    note = f"批量匯入修正 (原:{current_qty} -> 新:{new_qty})"
                
                add_transaction(doc_type, str(date.today()), sku, target_wh, abs(diff), "系統匯入", note)
                update_count += 1
            else:
                skip_count += 1
        return True, f"✅ 更新完成！已更新 {update_count} 筆，{skip_count} 筆無變動。"
    except Exception as e: return False, str(e)

def get_history(doc_type_filter=None, start_date=None, end_date=None):
    conn = get_connection()
    query = """
    SELECT h.date as '日期', h.doc_type as '單據類型', h.doc_no as '單號',
           p.series as '系列', p.category as '分類', p.name as '品名', p.spec as '規格',
           h.sku as '貨號', h.warehouse as '倉庫', h.qty as '數量', 
           h.user as '經手人', h.note as '備註'
    FROM history h
    LEFT JOIN products p ON h.sku = p.sku
    WHERE 1=1
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
    
    try:
        df = pd.read_sql(query, conn, params=params)
    except:
        df = pd.DataFrame()
    conn.close()
    return df

def get_period_summary(start_date, end_date):
    conn = get_connection()
    query = """
    SELECT h.sku, h.doc_type, SUM(h.qty) as total_qty
    FROM history h
    WHERE h.date BETWEEN ? AND ?
    GROUP BY h.sku, h.doc_type
    """
    try:
        df_raw = pd.read_sql(query, conn, params=(str(start_date), str(end_date)))
        if df_raw.empty: return pd.DataFrame()
        
        pivot = df_raw.pivot(index='sku', columns='doc_type', values='total_qty').fillna(0)
        for col in ['進貨', '銷售出貨', '製造入庫', '製造領料']:
            if col not in pivot.columns: pivot[col] = 0.0
            
        df_prod = pd.read_sql("SELECT sku, name, category, spec FROM products", conn)
        result = pd.merge(df_prod, pivot, on='sku', how='inner')
        
        result = result.rename(columns={
            'sku': '貨號', 'name': '品名', 'category': '分類', 'spec': '規格',
            '進貨': '期間進貨量', '銷售出貨': '期間出貨量',
            '製造入庫': '期間生產量', '製造領料': '期間領料量'
        })
        
        cols = ['貨號', '分類', '品名', '規格', '期間進貨量', '期間出貨量', '期間生產量', '期間領料量']
        return result[[c for c in cols if c in result.columns]]
        
    except Exception as e:
        return pd.DataFrame()
    finally:
        conn.close()

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
    page = st.radio("前往", [
        "📦 商品管理 (建檔/匯入)", 
        "📥 進貨作業", 
        "🚚 出貨作業", 
        "🔨 製造作業",
        "⚖️ 庫存盤點",
        "📊 報表查詢"
    ])
    
    st.divider()
    if st.button("🔴 初始化/重置資料庫"):
        reset_db()
        st.cache_data.clear()
        st.success("資料庫已重置！請重新建檔。")
        time.sleep(1)
        st.rerun()

# ------------------------------------------------------------------
# 1. 商品管理
# ------------------------------------------------------------------
if page == "📦 商品管理 (建檔/匯入)":
    st.subheader("📦 商品資料維護")
    
    tab1, tab2, tab3 = st.tabs(["✨ 單筆建檔", "📂 匯入商品資料", "📥 匯入期初庫存"])
    
    with tab1:
        with st.form("add_prod"):
            c1, c2 = st.columns(2)
            sku = c1.text_input("貨號 (SKU) *必填", placeholder="例如: ST-001")
            name = c2.text_input("品名 *必填")
            c3, c4, c5 = st.columns(3)
            cat = c3.selectbox("分類", CATEGORIES)
            ser = c4.selectbox("系列", SERIES)
            spec = c5.text_input("規格/尺寸")
            if st.form_submit_button("新增商品"):
                if sku and name:
                    success, msg = add_product(sku, name, cat, ser, spec)
                    if success: st.success(f"商品 {name} 建立成功！"); time.sleep(1); st.rerun()
                    else: st.error(msg)
                else: st.error("貨號與品名為必填！")

    with tab2:
        st.info("請上傳 Excel (欄位：`貨號`, `品名`, `分類`, `系列`, `規格`)")
        up = st.file_uploader("上傳商品清單", type=['xlsx', 'csv'], key='prod_up')
        if up and st.button("開始匯入商品"):
            try:
                df = pd.read_csv(up) if up.name.endswith('.csv') else pd.read_excel(up)
                df.columns = [str(c).strip() for c in df.columns]
                rename_map = {}
                for c in df.columns:
                    if c in ['SKU', '編號', '料號']: rename_map[c] = '貨號'
                    if c in ['名稱', '商品名稱']: rename_map[c] = '品名'
                    if c in ['類別', 'Category']: rename_map[c] = '分類'
                    if c in ['Series']: rename_map[c] = '系列'
                    if c in ['尺寸', 'Spec']: rename_map[c] = '規格'
                df = df.rename(columns=rename_map)
                
                count = 0
                if '貨號' in df.columns and '品名' in df.columns:
                    for _, row in df.iterrows():
                        s = str(row.get('貨號', '')).strip()
                        n = str(row.get('品名', '')).strip()
                        if s and n:
                            add_product(
                                s, n, 
                                str(row.get('分類', '未分類')), 
                                str(row.get('系列', '未分類')), 
                                str(row.get('規格', ''))
                            )
                            count += 1
                    st.success(f"成功匯入 {count} 筆商品資料！")
                    time.sleep(1); st.rerun()
                else: st.error("Excel 缺少 `貨號` 或 `品名` 欄位")
            except Exception as e: st.error(f"匯入失敗: {e}")

    with tab3:
        st.markdown("### 📥 批量匯入庫存")
        st.info("請上傳包含 `貨號` 與 `數量` 的 Excel 檔。")
        wh_batch = st.selectbox("預設入庫倉庫", WAREHOUSES, key="wh_init")
        up_stock = st.file_uploader("上傳庫存盤點表", type=['xlsx', 'csv'], key='stock_up')
        if up_stock and st.button("開始匯入庫存"):
            success, msg = process_batch_stock_update(up_stock, wh_batch)
            if success: st.success(msg); time.sleep(2); st.rerun()
            else: st.error(msg)

    st.divider()
    st.markdown("#### 目前商品清單")
    df_prod = get_all_products()
    st.dataframe(df_prod, use_container_width=True)

# ------------------------------------------------------------------
# 2. 進貨作業
# ------------------------------------------------------------------
elif page == "📥 進貨作業":
    st.subheader("📥 進貨入庫")
    prods = get_all_products()
    if prods.empty: st.warning("請先建立商品資料！")
    else:
        prods['label'] = prods['sku'] + " | " + prods['name']
        with st.form("in_stock"):
            c1, c2 = st.columns([2, 1])
            sel_prod = c1.selectbox("選擇商品", prods['label'])
            wh = c2.selectbox("入庫倉庫", WAREHOUSES, index=0)
            c3, c4 = st.columns(2)
            qty = c3.number_input("數量", min_value=1, value=1)
            date_val = c4.date_input("日期", date.today())
            
            user = st.selectbox("經手人", KEYERS)
            note = st.text_input("備註")
            
            if st.form_submit_button("確認進貨", type="primary"):
                target_sku = sel_prod.split(" | ")[0]
                if add_transaction("進貨", str(date_val), target_sku, wh, qty, user, note):
                    st.success("進貨成功！")
                    time.sleep(0.5); st.rerun()

        st.divider()
        st.markdown("#### 📜 最近進貨紀錄")
        df_hist = get_history(doc_type_filter="進貨")
        st.dataframe(df_hist, use_container_width=True)

# ------------------------------------------------------------------
# 3. 出貨作業
# ------------------------------------------------------------------
elif page == "🚚 出貨作業":
    st.subheader("🚚 銷售出貨")
    prods = get_all_products()
    if prods.empty: st.warning("無商品資料")
    else:
        prods['label'] = prods['sku'] + " | " + prods['name']
        with st.form("out_stock"):
            c1, c2 = st.columns([2, 1])
            sel_prod = c1.selectbox("選擇商品", prods['label'])
            wh = c2.selectbox("出貨倉庫", WAREHOUSES, index=2)
            c3, c4 = st.columns(2)
            qty = c3.number_input("數量", min_value=1, value=1)
            date_val = c4.date_input("日期", date.today())
            
            user = st.selectbox("經手人", KEYERS)
            note = st.text_input("訂單編號 / 備註")
            
            if st.form_submit_button("確認出貨", type="primary"):
                target_sku = sel_prod.split(" | ")[0]
                if add_transaction("銷售出貨", str(date_val), target_sku, wh, qty, user, note):
                    st.success("出貨成功！")
                    time.sleep(0.5); st.rerun()

        st.divider()
        st.markdown("#### 📜 最近出貨紀錄")
        df_hist = get_history(doc_type_filter="銷售出貨")
        st.dataframe(df_hist, use_container_width=True)

# ------------------------------------------------------------------
# 4. 製造作業
# ------------------------------------------------------------------
elif page == "🔨 製造作業":
    st.subheader("🔨 生產管理")
    prods = get_all_products()
    if not prods.empty:
        prods['label'] = prods['sku'] + " | " + prods['name']
        t1, t2 = st.tabs(["領料 (扣庫存)", "完工 (加庫存)"])
        
        with t1:
            with st.form("mo_out"):
                sel = st.selectbox("原料", prods['label'], key='m1')
                wh = st.selectbox("領料倉", WAREHOUSES, key='m2')
                qty = st.number_input("領用量", 1, key='m3')
                if st.form_submit_button("確認領料"):
                    sku = sel.split(" | ")[0]
                    add_transaction("製造領料", str(date.today()), sku, wh, qty, "工廠", "領料")
                    st.success("已扣除原料庫存"); time.sleep(0.5); st.rerun()

        with t2:
             with st.form("mo_in"):
                sel = st.selectbox("成品", prods['label'], key='p1')
                wh = st.selectbox("入庫倉", WAREHOUSES, key='p2')
                qty = st.number_input("產出量", 1, key='p3')
                if st.form_submit_button("完工入庫"):
                    sku = sel.split(" | ")[0]
                    add_transaction("製造入庫", str(date.today()), sku, wh, qty, "工廠", "完工")
                    st.success("成品已入庫"); time.sleep(0.5); st.rerun()

        st.divider()
        st.markdown("#### 📜 最近製造紀錄")
        df_hist = get_history(doc_type_filter=["製造領料", "製造入庫"])
        st.dataframe(df_hist, use_container_width=True)
    else: st.warning("請先建立商品資料！")

# ------------------------------------------------------------------
# 5. 庫存盤點
# ------------------------------------------------------------------
elif page == "⚖️ 庫存盤點":
    st.subheader("⚖️ 庫存調整")
    t1, t2 = st.tabs(["👋 單筆調整", "📂 批量盤點匯入"])
    prods = get_all_products()
    
    with t1:
        if not prods.empty:
            prods['label'] = prods['sku'] + " | " + prods['name']
            
            # 獲取過濾後的歷史原因
            reason_options = get_distinct_reasons()
            reason_options.append("➕ 手動輸入新原因")
            
            with st.form("adj"):
                c1, c2 = st.columns(2)
                sel = c1.selectbox("商品", prods['label'])
                wh = c2.selectbox("倉庫", WAREHOUSES)
                
                c3, c4 = st.columns(2)
                action = c3.radio("動作", ["增加 (+)", "減少 (-)"], horizontal=True)
                qty = c4.number_input("調整數量", 1)
                
                sel_reason = st.selectbox("調整原因", reason_options)
                
                if sel_reason == "➕ 手動輸入新原因":
                    final_reason = st.text_input("請輸入新原因")
                else:
                    final_reason = sel_reason
                
                if st.form_submit_button("提交調整"):
                    if not final_reason:
                        st.error("請輸入調整原因")
                    else:
                        sku = sel.split(" | ")[0]
                        type_name = "庫存調整(加)" if action == "增加 (+)" else "庫存調整(減)"
                        add_transaction(type_name, str(date.today()), sku, wh, qty, "管理員", final_reason)
                        st.success("調整完成！")
                        time.sleep(1); st.rerun()
                    
    with t2:
        st.markdown("### 📥 上傳盤點結果")
        st.info("上傳 Excel，系統將自動比對庫存差異並產生調整單。")
        wh_batch = st.selectbox("預設盤點倉庫", WAREHOUSES, key="wh_batch")
        up_stock = st.file_uploader("上傳盤點表", type=['xlsx', 'csv'], key='stock_up_batch')
        if up_stock and st.button("開始更新庫存"):
            success, msg = process_batch_stock_update(up_stock, wh_batch)
            if success: st.success(msg); time.sleep(2); st.rerun()
            else: st.error(msg)
    
    st.divider()
    st.markdown("### 📦 目前即時庫存")
    st.dataframe(get_stock_overview(), use_container_width=True)

# ------------------------------------------------------------------
# 6. 報表查詢
# ------------------------------------------------------------------
elif page == "📊 報表查詢":
    st.subheader("📊 數據報表中心")
    
    t1, t2, t3 = st.tabs(["📦 庫存總表", "📅 期間進銷存統計", "📜 分類明細下載"])
    
    with t1:
        df = get_stock_overview()
        st.dataframe(df, use_container_width=True)
        if not df.empty:
            st.download_button("📥 下載庫存現況表.xlsx", to_excel_download(df), f"Stock_{date.today()}.xlsx")

    with t2:
        st.markdown("##### 選擇統計期間")
        c1, c2 = st.columns(2)
        d_start = c1.date_input("開始日期", date.today().replace(day=1))
        d_end = c2.date_input("結束日期", date.today())
        
        if st.button("生成期間報表"):
            df_period = get_period_summary(d_start, d_end)
            if not df_period.empty:
                st.dataframe(df_period, use_container_width=True)
                st.download_button("📥 下載期間統計表.xlsx", to_excel_download(df_period), f"Report_{d_start}_{d_end}.xlsx")
            else:
                st.info("此期間無交易紀錄")

    with t3:
        st.markdown("##### 下載詳細流水帳")
        c1, c2, c3, c4 = st.columns(4)
        with c1:
            if st.button("📥 下載【進貨】明細"):
                df = get_history(doc_type_filter="進貨")
                st.download_button("點此下載", to_excel_download(df), "Inbound_Logs.xlsx")
        with c2:
            if st.button("📥 下載【出貨】明細"):
                df = get_history(doc_type_filter="銷售出貨")
                st.download_button("點此下載", to_excel_download(df), "Outbound_Logs.xlsx")
        with c3:
            if st.button("📥 下載【製造】明細"):
                df = get_history(doc_type_filter=["製造領料", "製造入庫"])
                st.download_button("點此下載", to_excel_download(df), "Manufacturing_Logs.xlsx")
        with c4:
            if st.button("📜 下載【完整流水帳】"):
                df = get_history()
                st.download_button("點此下載", to_excel_download(df), "Full_Logs.xlsx")
