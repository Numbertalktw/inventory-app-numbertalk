# =========================================================
# V3-FULL（版本 A）全功能 ERP 製造庫存系統
# =========================================================
# 作者：ChatGPT 定制版本
# 功能：SKU 規則、自動編碼、進貨、出貨、製造、盤點、主管後台、財務流向
# 儲存方式：CSV + SQLite（雙儲存）
# 特點：
#   - 乾淨穩定架構（MVC）
#   - 所有欄位標準化
#   - 加權平均法庫存
#   - 速度快、不崩潰、不白頁
# =========================================================

import streamlit as st
import pandas as pd
import sqlite3
import time
import io
import os
import re
from datetime import datetime, date
from pandas.api.types import (
    is_numeric_dtype, is_object_dtype, 
    is_categorical_dtype, is_datetime64_any_dtype
)

# =========================================================
# 0. Streamlit 設定 (務必放最上方)
# =========================================================
st.set_page_config(
    page_title="製造庫存系統 V3-Full",
    page_icon="🏭",
    layout="wide"
)

# =========================================================
# 1. 系統常數（欄位定義 / 檔案名稱）
# =========================================================

INVENTORY_FILE = "inventory_v3_full.csv"
HISTORY_FILE   = "history_v3_full.csv"
RULES_FILE     = "sku_rules_v3.xlsx"
DB_FILE        = "inventory_v3_full.db"

ADMIN_PASSWORD = "8888"

WAREHOUSES = ["Wen", "千畇", "James", "Imeng"]

# --- 流水帳欄位（欄位總表）---
HISTORY_COLUMNS = [
    "單據類型", "單號", "日期",
    "系列", "分類", "品名", "規格",
    "貨號", "批號",
    "倉庫", "數量", "Key單者",
    "廠商", 
    "訂單單號", "出貨日期", "貨號備註", "運費",
    "款項結清", "工資", "發票",
    "備註",
    "進貨總成本"
]

NUMERIC_COLS = ["數量", "運費", "工資", "進貨總成本"]

# --- 庫存欄位 ---
INVENTORY_COLUMNS = [
    "系列", "分類", "品名", "規格", 
    "貨號", "總庫存", "均價",
    "庫存_Wen", "庫存_千畇", "庫存_James", "庫存_Imeng"
]

# 預設規則 → 可由 Excel 規則檔覆蓋
DEFAULT_SKU_RULES = {
    "category": pd.DataFrame(columns=["名稱", "代碼"]),
    "series":   pd.DataFrame(columns=["名稱", "代碼"]),
    "name":     pd.DataFrame(columns=["名稱", "代碼"]),
    "spec":     pd.DataFrame(columns=["名稱", "代碼"])
}

# =========================================================
# 2. 工具函式（型別處理 / 防呆）
# =========================================================

def safe_float(v):
    """安全轉換為 float，失敗回傳 0"""
    try:
        if pd.isna(v) or v == "":
            return 0.0
        return float(str(v).replace(",", ""))
    except:
        return 0.0

def safe_str(v):
    """安全轉換為 str"""
    if pd.isna(v):
        return ""
    return str(v).strip()

# =========================================================
# 3. SQLite + CSV 讀寫
# =========================================================

def db_connect():
    return sqlite3.connect(DB_FILE)

def save_to_db(inv_df, hist_df):
    conn = db_connect()
    inv_df.to_sql("inventory", conn, if_exists="replace", index=False)
    hist_df.to_sql("history", conn, if_exists="replace", index=False)
    conn.close()

def load_from_db():
    if not os.path.exists(DB_FILE):
        return None, None
    try:
        conn = db_connect()
        inv = pd.read_sql("SELECT * FROM inventory", conn)
        hist = pd.read_sql("SELECT * FROM history", conn)
        conn.close()
        return inv, hist
    except:
        return None, None

# =========================================================
# 4. SKU 規則讀取 + 自動編碼
# =========================================================

def load_sku_rules():
    """讀取 SKU Excel 規則"""
    if not os.path.exists(RULES_FILE):
        return DEFAULT_SKU_RULES.copy()

    try:
        xls = pd.ExcelFile(RULES_FILE)
        rules = {}
        mapping = {
            "類別規則": "category",
            "系列規則": "series",
            "品名規則": "name",
            "規格規則": "spec",
        }
        for sheet_display, key in mapping.items():
            if sheet_display in xls.sheet_names:
                df = pd.read_excel(xls, sheet_display).astype(str)
                df.columns = ["名稱", "代碼"]
                rules[key] = df
            else:
                rules[key] = pd.DataFrame(columns=["名稱", "代碼"])
        return rules
    except:
        return DEFAULT_SKU_RULES.copy()


def sku_rule_lookup(rules_df, text):
    """依規則表找代碼 → fallback 取中文或數字特徵"""
    if rules_df is None or rules_df.empty:
        return ""

    # 完全比對
    match = rules_df[rules_df["名稱"] == text]
    if not match.empty:
        return safe_str(match.iloc[0]["代碼"]).upper()

    # 模糊比對
    for _, row in rules_df.iterrows():
        if safe_str(row["名稱"]) in safe_str(text):
            return safe_str(row["代碼"]).upper()

    # fallback：抓數字 or 前兩碼
    nums = re.findall(r"\d+", safe_str(text))
    if nums:
        return nums[0]
    return safe_str(text)[:2].upper()


def generate_sku(category, series, name, spec):
    """SKU = CAT-SER-NAME-SPEC"""
    rules = st.session_state["sku_rules"]

    cat_code = sku_rule_lookup(rules["category"], category)
    ser_code = sku_rule_lookup(rules["series"], series)
    name_code = sku_rule_lookup(rules["name"], name)
    spec_code = sku_rule_lookup(rules["spec"], spec)

    return f"{cat_code}-{ser_code}-{name_code}-{spec_code}"

# =========================================================
# 5. 標準化資料（History / Inventory）
# =========================================================

def normalize_history(df):
    """確保欄位齊全 + 正確型別"""
    df = df.copy()

    for col in HISTORY_COLUMNS:
        if col not in df.columns:
            df[col] = "" if col not in NUMERIC_COLS else 0.0

    # 數字欄位
    for col in NUMERIC_COLS:
        df[col] = df[col].apply(safe_float)

    # 日期欄位 → 字串 YYYY-MM-DD
    df["日期"] = df["日期"].astype(str).apply(lambda x: safe_str(x)[:10])

    # 全轉成字串
    for col in df.columns:
        if col not in NUMERIC_COLS:
            df[col] = df[col].astype(str)

    return df[HISTORY_COLUMNS]


def normalize_inventory(df):
    df = df.copy()
    for col in INVENTORY_COLUMNS:
        if col not in df.columns:
            df[col] = 0 if "庫存" in col or col == "均價" else ""

    df["貨號"] = df["貨號"].astype(str)

    # 數字欄位
    for col in ["總庫存", "均價"] + [f"庫存_{w}" for w in WAREHOUSES]:
        df[col] = df[col].apply(safe_float)

    return df[INVENTORY_COLUMNS]

# =========================================================
# 6. 載入資料（重點：優先 SQLite）
# =========================================================

def load_data():
    # 先試 SQLite
    inv, hist = load_from_db()

    # 庫存
    if inv is None:
        if os.path.exists(INVENTORY_FILE):
            inv = pd.read_csv(INVENTORY_FILE)
        else:
            inv = pd.DataFrame(columns=INVENTORY_COLUMNS)

    if hist is None:
        if os.path.exists(HISTORY_FILE):
            hist = pd.read_csv(HISTORY_FILE)
        else:
            hist = pd.DataFrame(columns=HISTORY_COLUMNS)

    return normalize_inventory(inv), normalize_history(hist)

# =========================================================
# 7. 庫存重算（加權平均法）
# =========================================================

def sort_inventory(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    df = df.copy()
    for c in ["系列", "分類", "品名", "規格"]:
        if c in df.columns:
            df[c] = df[c].fillna("").astype(str)
    return df.sort_values(
        by=[c for c in ["系列", "分類", "品名", "規格", "貨號"] if c in df.columns],
        ascending=True
    ).reset_index(drop=True)


def recalculate_inventory(hist_df: pd.DataFrame, inv_df: pd.DataFrame) -> pd.DataFrame:
    """依歷史流水帳重算庫存（加權平均成本），適用所有單據類型"""

    hist_df = normalize_history(hist_df)
    inv_df = normalize_inventory(inv_df)

    # --- 先補齊歷史帳中有，但 inventory 沒有的貨號 ---
    hist_skus = set(hist_df["貨號"].astype(str))
    inv_skus = set(inv_df["貨號"].astype(str))
    new_skus = hist_skus - inv_skus

    if new_skus:
        extra = (
            hist_df[hist_df["貨號"].astype(str).isin(new_skus)][
                ["貨號", "系列", "分類", "品名", "規格"]
            ]
            .drop_duplicates("貨號")
            .copy()
        )
        for col in INVENTORY_COLUMNS:
            if col not in extra.columns:
                extra[col] = 0 if ("庫存" in col or col == "均價") else ""
        inv_df = pd.concat([inv_df, extra[INVENTORY_COLUMNS]], ignore_index=True)

    # --- 初始化所有庫存數值 ---
    for col in ["總庫存", "均價"] + [f"庫存_{w}" for w in WAREHOUSES]:
        inv_df[col] = 0.0

    # --- 逐 SKU 計算 ---
    for idx, row in inv_df.iterrows():
        sku = str(row["貨號"])
        sub_hist = hist_df[hist_df["貨號"].astype(str) == sku]

        total_qty = 0.0
        total_val = 0.0
        w_stock = {w: 0.0 for w in WAREHOUSES}

        for _, h in sub_hist.iterrows():
            qty = safe_float(h["數量"])
            cost_total = safe_float(h["進貨總成本"])
            doc_type = safe_str(h["單據類型"])
            wh = safe_str(h["倉庫"])
            if wh not in WAREHOUSES:
                wh = WAREHOUSES[0]

            # 入庫類
            if doc_type in ["進貨", "製造入庫", "調整入庫", "期初建檔", "庫存調整(加)"]:
                total_qty += qty
                if cost_total > 0:
                    total_val += cost_total
                w_stock[wh] += qty

            # 出庫類
            elif doc_type in ["銷售出貨", "製造領料", "調整出庫", "庫存調整(減)"]:
                avg_cost = (total_val / total_qty) if total_qty > 0 else 0.0
                total_qty -= qty
                total_val -= qty * avg_cost
                if total_qty < 0:
                    total_qty = 0
                if total_val < 0:
                    total_val = 0
                w_stock[wh] -= qty

        inv_df.at[idx, "總庫存"] = total_qty
        inv_df.at[idx, "均價"] = (total_val / total_qty) if total_qty > 0 else 0.0
        for w in WAREHOUSES:
            inv_df.at[idx, f"庫存_{w}"] = w_stock[w]

    return sort_inventory(normalize_inventory(inv_df))


# =========================================================
# 8. 匯出 / 匯入工具
# =========================================================

def get_safe_view(df: pd.DataFrame) -> pd.DataFrame:
    """隱藏金額相關欄位，用於前台顯示"""
    if df is None or df.empty:
        return df
    sensitive = ["進貨總成本", "均價", "工資", "款項結清"]
    cols = [c for c in df.columns if c not in sensitive]
    return df[cols].copy()


def convert_single_sheet_to_excel(df: pd.DataFrame, sheet_name="Sheet1") -> bytes:
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name=sheet_name)
    return output.getvalue()


def convert_to_excel_all_sheets(inv_df: pd.DataFrame, hist_df: pd.DataFrame) -> bytes:
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        inv_df.to_excel(writer, index=False, sheet_name="庫存總表")

        if hist_df is not None and not hist_df.empty:
            df = hist_df.copy()
            df.to_excel(writer, index=False, sheet_name="完整流水帳")

            if "單據類型" in df.columns:
                df_in = df[df["單據類型"] == "進貨"]
                df_in.to_excel(writer, index=False, sheet_name="進貨紀錄")

                df_mfg = df[df["單據類型"].str.contains("製造", na=False)]
                df_mfg.to_excel(writer, index=False, sheet_name="製造紀錄")

                df_out = df[df["單據類型"].isin(["銷售出貨"])]
                df_out.to_excel(writer, index=False, sheet_name="銷貨紀錄")

    return output.getvalue()


def process_rules_upload(file_obj):
    """上傳 SKU 規則 Excel：四個工作表"""
    try:
        xls = pd.ExcelFile(file_obj)
        mapping = {
            "類別規則": "category",
            "系列規則": "series",
            "品名規則": "name",
            "規格規則": "spec",
        }
        rules = {}
        found_msgs = []
        for sheet_display, key in mapping.items():
            sheet_name = None
            for s in xls.sheet_names:
                if sheet_display in s:
                    sheet_name = s
                    break
            if sheet_name:
                df = pd.read_excel(xls, sheet_name).astype(str)
                if df.shape[1] >= 2:
                    df = df.iloc[:, :2]
                    df.columns = ["名稱", "代碼"]
                else:
                    df = pd.DataFrame(columns=["名稱", "代碼"])
                rules[key] = df
                found_msgs.append(f"{sheet_display}")
            else:
                rules[key] = pd.DataFrame(columns=["名稱", "代碼"])

        msg = "、".join(found_msgs) if found_msgs else "未偵測到任何規則分頁"
        return rules, msg
    except Exception as e:
        return None, str(e)


def save_rules_to_excel(rules_dict):
    """將目前規則寫回 Excel"""
    mapping = {
        "category": "類別規則",
        "series": "系列規則",
        "name": "品名規則",
        "spec": "規格規則",
    }
    with pd.ExcelWriter(RULES_FILE, engine="openpyxl") as writer:
        for key, df in rules_dict.items():
            sheet_name = mapping.get(key, key)
            df.to_excel(writer, index=False, sheet_name=sheet_name)


def process_product_upload(file_obj):
    """匯入商品基本資料（貨號、品名、系列、分類、規格）"""
    try:
        if file_obj.name.endswith(".csv"):
            dfs = [pd.read_csv(file_obj)]
        else:
            xls = pd.ExcelFile(file_obj)
            dfs = [pd.read_excel(xls, s) for s in xls.sheet_names]

        final = pd.DataFrame()
        for df in dfs:
            df = df.copy()
            df.columns = [safe_str(c) for c in df.columns]

            rename = {}
            for col in df.columns:
                col_s = safe_str(col)
                if col_s in ["名稱", "商品名稱", "品名", "Product Name"]:
                    rename[col] = "品名"
                elif col_s in ["SKU", "貨號", "編號", "Item Code"]:
                    rename[col] = "貨號"
                elif col_s in ["系列", "Series"]:
                    rename[col] = "系列"
                elif col_s in ["類別", "分類", "Category", "群組"]:
                    rename[col] = "分類"
                elif col_s in ["規格", "尺寸", "Spec"]:
                    rename[col] = "規格"

            df = df.rename(columns=rename)
            if "貨號" in df.columns and "品名" in df.columns:
                for c in ["系列", "分類", "規格"]:
                    if c not in df.columns:
                        df[c] = ""
                subset = df[["貨號", "品名", "系列", "分類", "規格"]].copy()
                final = pd.concat([final, subset], ignore_index=True)

        if final.empty:
            return None, "未找到有效欄位（需要至少『貨號』『品名』）"

        final = final.astype(str).drop_duplicates(subset=["貨號"])
        return final, "OK"
    except Exception as e:
        return None, f"匯入失敗：{e}"


def process_opening_stock_upload(file_obj, default_wh):
    """期初庫存匯入 -> 轉成 History 的 期初建檔 單據"""
    try:
        if file_obj.name.endswith(".csv"):
            dfs = [pd.read_csv(file_obj)]
        else:
            xls = pd.ExcelFile(file_obj)
            dfs = [pd.read_excel(xls, s) for s in xls.sheet_names]

        recs = []
        inv = st.session_state["inventory"]

        for df in dfs:
            df = df.copy()
            df.columns = [safe_str(c) for c in df.columns]

            rename = {}
            for col in df.columns:
                c = safe_str(col)
                if c in ["SKU", "貨號", "編號"]:
                    rename[col] = "貨號"
                elif c in ["庫存", "現有庫存", "數量", "Qty"]:
                    rename[col] = "數量"
                elif c in ["成本", "進貨總成本", "Cost", "總成本"]:
                    rename[col] = "進貨總成本"
                elif c in ["品名", "名稱"]:
                    rename[col] = "品名"
                elif c in ["系列", "Series"]:
                    rename[col] = "系列"
                elif c in ["類別", "分類", "Category"]:
                    rename[col] = "分類"
                elif c in ["規格", "尺寸", "Spec"]:
                    rename[col] = "規格"

            df = df.rename(columns=rename)

            if "貨號" not in df.columns or "數量" not in df.columns:
                continue

            for _, r in df.iterrows():
                sku = safe_str(r["貨號"])
                if not sku:
                    continue
                qty = safe_float(r["數量"])
                if qty <= 0:
                    continue

                wh = default_wh
                if "倉庫" in df.columns and safe_str(r["倉庫"]) in WAREHOUSES:
                    wh = safe_str(r["倉庫"])

                exist = inv[inv["貨號"] == sku]
                if not exist.empty:
                    series = safe_str(exist.iloc[0]["系列"])
                    cat = safe_str(exist.iloc[0]["分類"])
                    name = safe_str(exist.iloc[0]["品名"])
                    spec = safe_str(exist.iloc[0]["規格"])
                else:
                    series = safe_str(r.get("系列", "期初"))
                    cat = safe_str(r.get("分類", "期初"))
                    name = safe_str(r.get("品名", f"未命名-{sku}"))
                    spec = safe_str(r.get("規格", ""))

                rec = {
                    "單據類型": "期初建檔",
                    "單號": f"OPEN-{int(time.time())}-{sku}",
                    "日期": str(date.today()),
                    "系列": series,
                    "分類": cat,
                    "品名": name,
                    "規格": spec,
                    "貨號": sku,
                    "批號": f"INIT-{date.today():%Y%m%d}",
                    "倉庫": wh,
                    "數量": qty,
                    "Key單者": "期初匯入",
                    "進貨總成本": safe_float(r.get("進貨總成本", 0)),
                    "備註": "期初匯入",
                }
                for c in HISTORY_COLUMNS:
                    if c not in rec:
                        rec[c] = 0 if c in NUMERIC_COLS else ""

                recs.append(rec)

        if not recs:
            return None, "未找到任何有效期初資料"

        df_res = pd.DataFrame(recs)
        return normalize_history(df_res), "OK"
    except Exception as e:
        return None, f"匯入失敗：{e}"


def process_restore_upload(file_obj):
    """從備份 Excel（含 完整流水帳 工作表）還原"""
    try:
        df = pd.read_excel(file_obj, sheet_name="完整流水帳")
        return normalize_history(df)
    except Exception:
        return None


# =========================================================
# 9. 通用 UI 工具：篩選器
# =========================================================

def filter_dataframe(df: pd.DataFrame, key_prefix: str = "") -> pd.DataFrame:
    """通用篩選器（含全選、多欄位）"""
    if df is None or df.empty:
        return df

    df = df.copy()
    toggle = st.checkbox(
        "🔍 開啟資料篩選器",
        key=f"{key_prefix}_filter_toggle"
    )
    if not toggle:
        return df

    # 嘗試轉日期
    for col in df.columns:
        if is_object_dtype(df[col]):
            try:
                df[col] = pd.to_datetime(df[col])
            except Exception:
                pass

    with st.container():
        targets = st.multiselect(
            "選擇要篩選的欄位",
            df.columns,
            key=f"{key_prefix}_filter_columns"
        )

        for col in targets:
            c1, c2 = st.columns((1, 5))
            c1.write("↳")

            series = df[col]

            # 類別型 / 低基數欄位
            if is_categorical_dtype(series) or series.nunique() < 50:
                options = sorted(series.astype(str).unique())
                use_all = c2.checkbox(
                    f"{col} 全選",
                    value=True,
                    key=f"{key_prefix}_all_{col}"
                )
                if use_all:
                    selected = options
                else:
                    selected = c2.multiselect(
                        f"選擇 {col}",
                        options,
                        key=f"{key_prefix}_sel_{col}"
                    )
                df = df[df[col].astype(str).isin(selected)]

            # 數字欄位
            elif is_numeric_dtype(series):
                min_v = float(series.min())
                max_v = float(series.max())
                if min_v == max_v:
                    df = df[series == min_v]
                else:
                    step = (max_v - min_v) / 100
                    v_min, v_max = c2.slider(
                        f"{col} 範圍",
                        min_v,
                        max_v,
                        (min_v, max_v),
                        step=step,
                        key=f"{key_prefix}_rng_{col}",
                    )
                    df = df[series.between(v_min, v_max)]

            # 日期欄位
            elif is_datetime64_any_dtype(series):
                d_min = series.min().date()
                d_max = series.max().date()
                start, end = c2.date_input(
                    f"{col} 日期區間",
                    (d_min, d_max),
                    key=f"{key_prefix}_date_{col}",
                )
                if isinstance(start, date) and isinstance(end, date):
                    start_ts = pd.to_datetime(start)
                    end_ts = pd.to_datetime(end)
                    df = df[series.between(start_ts, end_ts)]

            # 文字模糊搜尋
            else:
                text = c2.text_input(
                    f"搜尋 {col} 包含文字",
                    key=f"{key_prefix}_txt_{col}",
                )
                if text:
                    df = df[series.astype(str).str.contains(text, case=False, na=False)]

    return df


# =========================================================
# 10. 動態選項（系列 / 分類 等）
# =========================================================

def get_dynamic_options(column_name: str, default_list):
    """讀取 inventory 當前欄位 + 預設 + 規則名稱"""
    options = set(default_list)

    inv = st.session_state.get("inventory", pd.DataFrame())
    if column_name in inv.columns:
        vals = inv[column_name].dropna().astype(str).tolist()
        options.update([v for v in vals if v.strip()])

    # 從規則中補值
    rule_key_map = {
        "系列": "series",
        "分類": "category",
    }
    rules = st.session_state.get("sku_rules", DEFAULT_SKU_RULES)
    if column_name in rule_key_map:
        key = rule_key_map[column_name]
        df_rule = rules.get(key)
        if df_rule is not None and not df_rule.empty:
            vals = df_rule["名稱"].dropna().astype(str).tolist()
            options.update([v for v in vals if v.strip()])

    result = sorted(options)
    result.append("➕ 手動輸入新資料")
    return result


# =========================================================
# 11. 資料儲存（CSV + SQLite）
# =========================================================

def save_data():
    """將目前 Session 中的 inventory / history 寫入 CSV + SQLite"""
    inv = normalize_inventory(st.session_state["inventory"])
    hist = normalize_history(st.session_state["history"])

    # CSV
    inv.to_csv(INVENTORY_FILE, index=False, encoding="utf-8-sig")
    hist.to_csv(HISTORY_FILE, index=False, encoding="utf-8-sig")

    # SQLite
    save_to_db(inv, hist)


# =========================================================
# 12. Session 初始化
# =========================================================

if "inventory" not in st.session_state or "history" not in st.session_state:
    inv, hist = load_data()
    st.session_state["inventory"] = inv
    st.session_state["history"] = hist

if "sku_rules" not in st.session_state:
    st.session_state["sku_rules"] = load_sku_rules()

# =========================================================
# 13. 其他小工具（單號 / 批號）
# =========================================================

# =========================================================
# 13. 其他小工具（單號 / 批號）
# =========================================================

def gen_batch_number(prefix: str = "BAT") -> str:
    return f"{prefix}-{datetime.now().strftime('%y%m%d%H%M')}"

def gen_mo_number() -> str:
    return f"MO-{datetime.now().strftime('%y%m%d-%H%M')}"


# =========================================================
# 14. 一些預設選項
# =========================================================

DEFAULT_SERIES = []
DEFAULT_CATEGORIES = []
DEFAULT_KEYERS = ["Wen", "千畇", "James", "Imeng", "小幫手"]


# =========================================================
# 15. 新增：可下載篩選結果的 filter_dataframe（新版）
# =========================================================

def filter_dataframe(df: pd.DataFrame, key_prefix="") -> pd.DataFrame:
    """全系統共用篩選器，支援匯出目前篩選後報表"""
    if df.empty:
        st.session_state["last_filtered_df"] = df
        return df

    modify = st.checkbox("🔍 開啟資料篩選器", key=f"{key_prefix}_filter_toggle")
    if not modify:
        st.session_state["last_filtered_df"] = df
        return df

    df = df.copy()

    # 嘗試日期轉換
    for col in df.columns:
        if is_object_dtype(df[col]):
            try:
                df[col] = pd.to_datetime(df[col])
            except:
                pass

    selected_cols = st.multiselect(
        "選擇篩選欄位",
        df.columns,
        key=f"{key_prefix}_cols"
    )

    for col in selected_cols:

        # 數字
        if is_numeric_dtype(df[col]):
            mn, mx = float(df[col].min()), float(df[col].max())
            r_min, r_max = st.slider(
                f"{col} 範圍",
                min_value=mn,
                max_value=mx,
                value=(mn, mx),
                key=f"{key_prefix}_{col}_range"
            )
            df = df[(df[col] >= r_min) & (df[col] <= r_max)]

        # 日期
        elif is_datetime64_any_dtype(df[col]):
            d_min, d_max = df[col].min().date(), df[col].max().date()
            start, end = st.date_input(
                f"{col} 日期範圍",
                (d_min, d_max),
                key=f"{key_prefix}_{col}_date"
            )
            df = df[(df[col].dt.date >= start) & (df[col].dt.date <= end)]

        # 文字搜尋
        else:
            txt = st.text_input(
                f"搜尋 {col}",
                key=f"{key_prefix}_{col}_txt"
            )
            if txt:
                df = df[df[col].astype(str).str.contains(txt, case=False)]

    # ★ 記錄起來供報表下載
    st.session_state["last_filtered_df"] = df

    return df


# =========================================================
# 16. 主畫面標題
# =========================================================

st.title("🏭 製造庫存系統 V3-Full")


# =========================================================
# 17. Sidebar（含報表下載中心 + 備份）★ 已升級
# =========================================================

with st.sidebar:
    st.header("部門功能導航")

    page = st.radio(
        "選擇作業",
        [
            "📦 商品建檔與維護",
            "📥 進貨",
            "🚚 銷售出貨",
            "🔨 製造生產",
            "⚖️ 庫存盤點與調整",
            "📊 總表監控 (主管)",
            "💰 成本與財務管理 (加密)",
        ],
        key="page_select",
    )

    st.markdown("---")
    st.subheader("📁 報表下載中心")

    inv_df = st.session_state["inventory"]
    hist_df = st.session_state["history"]

    # -----------------------------------------------------
    # 1. 單一報表（即時）
    # -----------------------------------------------------
    if not hist_df.empty:
        with st.expander("📥 單頁報表下載（即時）", expanded=False):

            st.download_button(
                "📊 庫存現況.xlsx",
                convert_single_sheet_to_excel(inv_df, "庫存總表"),
                file_name=f"Inventory_{date.today()}.xlsx"
            )

            df_in = hist_df[hist_df["單據類型"] == "進貨"]
            st.download_button(
                "📥 進貨紀錄.xlsx",
                convert_single_sheet_to_excel(df_in, "進貨紀錄"),
                file_name=f"Inbound_{date.today()}.xlsx"
            )

            df_out = hist_df[hist_df["單據類型"] == "銷售出貨"]
            st.download_button(
                "🚚 銷貨紀錄.xlsx",
                convert_single_sheet_to_excel(df_out, "銷貨紀錄"),
                file_name=f"Sales_{date.today()}.xlsx"
            )

            df_mfg = hist_df[hist_df["單據類型"].str.contains("製造", na=False)]
            st.download_button(
                "🔨 製造紀錄.xlsx",
                convert_single_sheet_to_excel(df_mfg, "製造紀錄"),
                file_name=f"Mfg_{date.today()}.xlsx"
            )

            st.download_button(
                "📜 完整流水帳.xlsx",
                convert_single_sheet_to_excel(hist_df, "完整流水帳"),
                file_name=f"History_{date.today()}.xlsx"
            )

    # -----------------------------------------------------
    # 2. 多工作表報表（含彙總）
    # -----------------------------------------------------
    with st.expander("📘 下載完整報表（多工作表 Excel）", expanded=False):

        df_wh_sum = inv_df[
            ["貨號", "品名"] + [f"庫存_{w}" for w in WAREHOUSES]
        ].copy()

        df_cat_sum = inv_df.groupby("分類")[["總庫存"]].sum().reset_index()

        output = io.BytesIO()
        with pd.ExcelWriter(output, engine="openpyxl") as writer:

            inv_df.to_excel(writer, index=False, sheet_name="庫存總表")
            hist_df[hist_df["單據類型"] == "進貨"].to_excel(
                writer, index=False, sheet_name="進貨紀錄"
            )
            hist_df[hist_df["單據類型"] == "銷售出貨"].to_excel(
                writer, index=False, sheet_name="銷貨紀錄"
            )
            hist_df[hist_df["單據類型"].str.contains("製造", na=False)].to_excel(
                writer, index=False, sheet_name="製造紀錄"
            )
            hist_df.to_excel(writer, index=False, sheet_name="完整流水帳")

            df_wh_sum.to_excel(writer, index=False, sheet_name="倉庫庫存彙總")
            df_cat_sum.to_excel(writer, index=False, sheet_name="分類庫存彙總")

        st.download_button(
            "📘 下載完整報表（含 6 個工作表）",
            output.getvalue(),
            file_name=f"FullReport_{date.today()}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

    # -----------------------------------------------------
    # 3. 匯出「篩選後」畫面資料
    # -----------------------------------------------------
    with st.expander("📄 匯出目前頁面篩選結果", expanded=False):

        if "last_filtered_df" in st.session_state:
            df_f = st.session_state["last_filtered_df"]

            st.download_button(
                "⬇ 匯出目前篩選結果.xlsx",
                convert_single_sheet_to_excel(df_f, "篩選後"),
                file_name=f"Filtered_{date.today()}.xlsx"
            )
        else:
            st.info("尚無可匯出的篩選結果。（請先在頁面使用篩選器）")

    st.markdown("---")
    if st.button("🔴 重置 Session（畫面怪怪時使用）", key="reset_session"):
        st.session_state.clear()
        st.rerun()


# =========================================================
# ★★★ 接下來：保留你原本 Part 3 的所有頁面 Section（商品建檔 / 出貨 / 製造 / 財務）
# =========================================================
# 你從這一行之後，繼續貼上你原本 Part 3 的剩餘部分即可。

# =========================================================
# 18. 頁面 3：🚚 銷售出貨
# =========================================================

elif page == "🚚 銷售出貨":
    st.subheader("🚚 銷售出貨")

    inv = st.session_state["inventory"]
    if inv.empty:
        st.warning("目前尚無商品資料，請先建立商品。")
    else:
        inv = sort_inventory(inv)
        inv["label"] = (
            inv["貨號"].astype(str)
            + " | "
            + inv["品名"].astype(str)
            + " | 庫存:"
            + inv["總庫存"].astype(int).astype(str)
        )

        with st.form("form_sales"):
            c1, c2 = st.columns([2, 1])
            sel_label = c1.selectbox("出貨商品", inv["label"], key="out_sel_product")
            wh = c2.selectbox("出貨倉庫", WAREHOUSES, key="out_wh")

            c3, c4, c5 = st.columns(3)
            qty = c3.number_input("出貨數量", min_value=1, value=1, key="out_qty")
            fee = c4.number_input("運費", min_value=0.0, value=0.0, key="out_fee")
            out_date = c5.date_input("出貨日期", value=date.today(), key="out_date")

            c6, c7 = st.columns(2)
            order_no = c6.text_input("訂單單號", key="out_order")
            keyer = c7.selectbox("Key 單者", DEFAULT_KEYERS, key="out_keyer")

            note = st.text_area("備註", key="out_note")

            submit = st.form_submit_button("✅ 建立出貨單", type="primary")

        if submit:
            sel_row = inv[inv["label"] == sel_label].iloc[0]
            sku = safe_str(sel_row["貨號"])

            rec = {
                "單據類型": "銷售出貨",
                "單號": datetime.now().strftime("%Y%m%d%H%M%S"),
                "日期": str(out_date),
                "出貨日期": str(out_date),
                "系列": safe_str(sel_row["系列"]),
                "分類": safe_str(sel_row["分類"]),
                "品名": safe_str(sel_row["品名"]),
                "規格": safe_str(sel_row["規格"]),
                "貨號": sku,
                "批號": "",
                "倉庫": wh,
                "數量": qty,
                "運費": fee,
                "Key單者": keyer,
                "訂單單號": order_no,
                "備註": note,
            }
            for c in HISTORY_COLUMNS:
                if c not in rec:
                    rec[c] = 0 if c in NUMERIC_COLS else ""

            hist = st.session_state["history"]
            hist = pd.concat([hist, pd.DataFrame([rec])], ignore_index=True)
            st.session_state["history"] = hist
            st.session_state["inventory"] = recalculate_inventory(hist, inv)
            save_data()
            st.success("✅ 出貨單已建立，庫存已更新。")

    st.markdown("---")
    if not hist_df.empty:
        df_view = hist_df[hist_df["單據類型"] == "銷售出貨"].copy()
        cols = [
            "單號",
            "訂單單號",
            "日期",
            "出貨日期",
            "系列",
            "分類",
            "品名",
            "規格",
            "貨號",
            "倉庫",
            "數量",
            "運費",
            "Key單者",
            "備註",
        ]
        df_view = df_view[[c for c in cols if c in df_view.columns]]
        df_view = filter_dataframe(df_view, key_prefix="out_list")
        st.dataframe(df_view, use_container_width=True)


# =========================================================
# 19. 頁面 4：🔨 製造生產
# =========================================================

elif page == "🔨 製造生產":
    st.subheader("🔨 製造生產紀錄")

    inv = st.session_state["inventory"]
    if inv.empty:
        st.warning("目前尚無商品資料，請先建立商品。")
    else:
        inv = sort_inventory(inv)
        inv["label"] = (
            inv["貨號"].astype(str)
            + " | "
            + inv["品名"].astype(str)
            + " | 庫存:"
            + inv["總庫存"].astype(int).astype(str)
        )

        tab_issue, tab_finish = st.tabs(["📤 領料", "📥 完工入庫"])

        # ------- 領料 -------
        with tab_issue:
            with st.form("form_mfg_issue"):
                c1, c2 = st.columns(2)
                mat_label = c1.selectbox("原料", inv["label"], key="mfg_issue_product")
                wh = c2.selectbox("從哪個倉庫領料", WAREHOUSES, key="mfg_issue_wh")

                c3, c4 = st.columns(2)
                qty = c3.number_input(
                    "領料數量", min_value=1, value=1, key="mfg_issue_qty"
                )
                mo_no = c4.text_input(
                    "工單單號（可自動產生）",
                    value=gen_mo_number(),
                    key="mfg_issue_mo",
                )

                keyer = st.selectbox("領料人 / Key 單者", DEFAULT_KEYERS, key="mfg_issue_keyer")

                submit_issue = st.form_submit_button("✅ 確認領料", type="primary")

            if submit_issue:
                mat_row = inv[inv["label"] == mat_label].iloc[0]
                sku = safe_str(mat_row["貨號"])

                rec = {
                    "單據類型": "製造領料",
                    "單號": datetime.now().strftime("%Y%m%d%H%M%S"),
                    "日期": str(date.today()),
                    "系列": safe_str(mat_row["系列"]),
                    "分類": safe_str(mat_row["分類"]),
                    "品名": safe_str(mat_row["品名"]),
                    "規格": safe_str(mat_row["規格"]),
                    "貨號": sku,
                    "批號": "",
                    "倉庫": wh,
                    "數量": qty,
                    "Key單者": keyer,
                    "訂單單號": mo_no,
                }
                for c in HISTORY_COLUMNS:
                    if c not in rec:
                        rec[c] = 0 if c in NUMERIC_COLS else ""

                hist = st.session_state["history"]
                hist = pd.concat([hist, pd.DataFrame([rec])], ignore_index=True)
                st.session_state["history"] = hist
                st.session_state["inventory"] = recalculate_inventory(hist, inv)
                save_data()
                st.success("✅ 製造領料已記錄。")

        # ------- 完工入庫 -------
        with tab_finish:
            with st.form("form_mfg_finish"):
                c1, c2 = st.columns(2)
                fin_label = c1.selectbox("成品", inv["label"], key="mfg_fin_product")
                wh_fin = c2.selectbox(
                    "入庫倉庫", WAREHOUSES, index=1, key="mfg_fin_wh"
                )

                c3, c4, c5 = st.columns(3)
                qty_fin = c3.number_input(
                    "完工數量", min_value=1, value=1, key="mfg_fin_qty"
                )
                batch = c4.text_input(
                    "批號", value=gen_batch_number("PD"), key="mfg_fin_batch"
                )
                mo_no_fin = c5.text_input(
                    "工單單號", value=gen_mo_number(), key="mfg_fin_mo"
                )

                keyer_fin = st.selectbox(
                    "Key 單者", DEFAULT_KEYERS, key="mfg_fin_keyer"
                )

                submit_fin = st.form_submit_button("✅ 完工入庫", type="primary")

            if submit_fin:
                fin_row = inv[inv["label"] == fin_label].iloc[0]
                sku = safe_str(fin_row["貨號"])

                rec = {
                    "單據類型": "製造入庫",
                    "單號": datetime.now().strftime("%Y%m%d%H%M%S"),
                    "日期": str(date.today()),
                    "系列": safe_str(fin_row["系列"]),
                    "分類": safe_str(fin_row["分類"]),
                    "品名": safe_str(fin_row["品名"]),
                    "規格": safe_str(fin_row["規格"]),
                    "貨號": sku,
                    "批號": batch,
                    "倉庫": wh_fin,
                    "數量": qty_fin,
                    "Key單者": keyer_fin,
                    "訂單單號": mo_no_fin,
                }
                for c in HISTORY_COLUMNS:
                    if c not in rec:
                        rec[c] = 0 if c in NUMERIC_COLS else ""

                hist = st.session_state["history"]
                hist = pd.concat([hist, pd.DataFrame([rec])], ignore_index=True)
                st.session_state["history"] = hist
                st.session_state["inventory"] = recalculate_inventory(hist, inv)
                save_data()
                st.success("✅ 完工入庫已記錄。")

    st.markdown("---")
    if not hist_df.empty:
        df_view = hist_df[hist_df["單據類型"].str.contains("製造", na=False)].copy()
        df_view = filter_dataframe(df_view, key_prefix="mfg_list")
        st.dataframe(df_view, use_container_width=True)


# =========================================================
# 20. 頁面 5：⚖️ 庫存盤點與調整
# =========================================================

elif page == "⚖️ 庫存盤點與調整":
    st.subheader("⚖️ 庫存盤點與調整")

    inv = st.session_state["inventory"]
    if inv.empty:
        st.warning("尚無商品資料。")
    else:
        inv = sort_inventory(inv)
        inv["label"] = (
            inv["貨號"].astype(str)
            + " | "
            + inv["品名"].astype(str)
            + " | 庫存:"
            + inv["總庫存"].astype(int).astype(str)
        )

        c1, c2 = st.columns([2, 1])
        sel_label = c1.selectbox("選擇商品", inv["label"], key="adj_sel_product")
        wh = c2.selectbox("盤點倉庫", WAREHOUSES, key="adj_wh")

        row = inv[inv["label"] == sel_label].iloc[0]
        current_qty = safe_float(row[f"庫存_{wh}"])
        st.metric("目前系統庫存", f"{int(current_qty)}")

        with st.form("form_adjust"):
            new_qty = st.number_input(
                "盤點實際數量", min_value=0, value=int(max(current_qty, 0)), key="adj_new_qty"
            )
            reason = st.text_input("調整原因", "盤點修正", key="adj_reason")
            submit_adj = st.form_submit_button("✅ 確認調整", type="primary")

        if submit_adj:
            diff = new_qty - current_qty
            if diff == 0:
                st.info("實際數量與系統相同，無需調整。")
            else:
                doc_type = "庫存調整(加)" if diff > 0 else "庫存調整(減)"
                qty = abs(diff)
                rec = {
                    "單據類型": doc_type,
                    "單號": f"ADJ-{int(time.time())}",
                    "日期": str(date.today()),
                    "系列": safe_str(row["系列"]),
                    "分類": safe_str(row["分類"]),
                    "品名": safe_str(row["品名"]),
                    "規格": safe_str(row["規格"]),
                    "貨號": safe_str(row["貨號"]),
                    "批號": "",
                    "倉庫": wh,
                    "數量": qty,
                    "Key單者": "盤點",
                    "備註": f"{reason} (原:{current_qty} → 新:{new_qty})",
                }
                for c in HISTORY_COLUMNS:
                    if c not in rec:
                        rec[c] = 0 if c in NUMERIC_COLS else ""

                hist = st.session_state["history"]
                hist = pd.concat([hist, pd.DataFrame([rec])], ignore_index=True)
                st.session_state["history"] = hist
                st.session_state["inventory"] = recalculate_inventory(hist, inv)
                save_data()
                st.success("✅ 庫存已更新。")


# =========================================================
# 21. 頁面 6：📊 總表監控 (主管)
# =========================================================

elif page == "📊 總表監控 (主管)":
    st.subheader("📊 總表監控（主管專用）")
    pwd = st.text_input("請輸入主管密碼", type="password", key="admin_pwd_monitor")
    if pwd == ADMIN_PASSWORD:
        st.success("✅ 密碼驗證成功。")

        tab_inv, tab_hist = st.tabs(["📦 庫存總表", "📜 完整流水帳"])

        with tab_inv:
            df_view = st.session_state["inventory"]
            df_view = filter_dataframe(df_view, key_prefix="mgr_inv")
            edited_inv = st.data_editor(
                df_view,
                num_rows="dynamic",
                use_container_width=True,
                key="mgr_inv_editor",
            )
            if st.button("💾 儲存庫存變更", key="mgr_save_inv"):
                st.session_state["inventory"] = normalize_inventory(edited_inv)
                save_data()
                st.success("✅ 庫存總表已更新。")

        with tab_hist:
            df_view = st.session_state["history"]
            df_view = filter_dataframe(df_view, key_prefix="mgr_hist")
            edited_hist = st.data_editor(
                df_view,
                num_rows="dynamic",
                use_container_width=True,
                key="mgr_hist_editor",
            )
            if st.button("💾 儲存流水帳變更並重算庫存", key="mgr_save_hist"):
                st.session_state["history"] = normalize_history(edited_hist)
                st.session_state["inventory"] = recalculate_inventory(
                    st.session_state["history"],
                    st.session_state["inventory"],
                )
                save_data()
                st.success("✅ 流水帳與庫存已重新計算。")
    elif pwd != "":
        st.error("❌ 密碼錯誤。")


# =========================================================
# 22. 頁面 7：💰 成本與財務管理 (加密)
# =========================================================

elif page == "💰 成本與財務管理 (加密)":
    st.subheader("💰 成本與財務管理（含進貨成本 / 工資 / 運費等）")
    pwd = st.text_input("請輸入管理員密碼", type="password", key="admin_pwd_fin")
    if pwd == ADMIN_PASSWORD:
        st.success("✅ 身份驗證成功。")

        tab_cost, tab_full = st.tabs(["💸 進貨成本補登", "📜 完整流水帳（含金額）"])

        # ---- 進貨成本補登 ----
        with tab_cost:
            df = st.session_state["history"]
            mask = (df["單據類型"] == "進貨") & (df["進貨總成本"] == 0)
            df_fix = df[mask].copy()
            if df_fix.empty:
                st.info("目前沒有進貨成本為 0 的單據。")
            else:
                df_fix = filter_dataframe(df_fix, key_prefix="fin_fix")
                edited_fix = st.data_editor(
                    df_fix,
                    num_rows="dynamic",
                    use_container_width=True,
                    key="fin_fix_editor",
                    column_config={
                        "進貨總成本": st.column_config.NumberColumn(required=True),
                    },
                )
                if st.button("💾 儲存進貨成本並重算庫存", key="btn_save_cost_fix"):
                    df.update(edited_fix)
                    st.session_state["history"] = normalize_history(df)
                    st.session_state["inventory"] = recalculate_inventory(
                        st.session_state["history"],
                        st.session_state["inventory"],
                    )
                    save_data()
                    st.success("✅ 已更新進貨成本與庫存。")

        # ---- 全部流水帳可編輯 ----
        with tab_full:
            df = st.session_state["history"]
            df_view = filter_dataframe(df, key_prefix="fin_full")
            edited_all = st.data_editor(
                df_view,
                num_rows="dynamic",
                use_container_width=True,
                key="fin_full_editor",
            )
            if st.button("💾 儲存全部變更並重算庫存", key="btn_save_fin_full"):
                st.session_state["history"] = normalize_history(edited_all)
                st.session_state["inventory"] = recalculate_inventory(
                    st.session_state["history"],
                    st.session_state["inventory"],
                )
                save_data()
                st.success("✅ 流水帳與庫存已更新。")
    elif pwd != "":
        st.error("❌ 密碼錯誤。")


