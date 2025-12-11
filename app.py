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
import sqlite3

# =========================================================
# 0. 基本設定
# =========================================================

st.set_page_config(page_title="製造庫存系統", layout="wide", page_icon="🏭")

PAGE_TITLE = "製造庫存系統"

# 檔案名稱
INVENTORY_CSV = "inventory_secure_v5.csv"
HISTORY_CSV = "history_secure_v5.csv"
DB_FILE = "inventory_secure_v5.db"

ADMIN_PASSWORD = "8888"  # 管理員/主管密碼

# 倉庫 (人員)
WAREHOUSES = ["Wen", "千畇", "James", "Imeng"]

# --- 核心流水帳 ---
HISTORY_COLUMNS = [
    "單據類型",
    "單號",
    "日期",
    "系列",
    "分類",
    "品名",
    "貨號",
    "批號",
    "倉庫",
    "數量",
    "Key單者",
    "廠商",
    "訂單單號",
    "出貨日期",
    "貨號備註",
    "運費",
    "款項結清",
    "工資",
    "發票",
    "備註",
    "進貨總成本",
]

NUM_HISTORY_COLS = ["數量", "進貨總成本", "運費", "工資"]

# --- 庫存狀態表 ---
INVENTORY_COLUMNS = [
    "貨號",
    "系列",
    "分類",
    "品名",
    "總庫存",
    "均價",
    "庫存_Wen",
    "庫存_千畇",
    "庫存_James",
    "庫存_Imeng",
]

DEFAULT_SERIES = ["原料", "半成品", "成品", "包材"]
DEFAULT_CATEGORIES = ["天然石", "金屬配件", "線材", "包裝材料", "完成品"]
DEFAULT_KEYERS = ["Wen", "千畇", "James", "Imeng", "小幫手"]

PREFIX_MAP = {
    "天然石": "ST",
    "金屬配件": "MT",
    "線材": "WR",
    "包裝材料": "PK",
    "完成品": "PD",
    "耗材": "OT",
}

# =========================================================
# 1. 共用工具函式
# =========================================================

def safe_float(x, default=0.0):
    """將任意值安全轉為 float，轉不出來就回 default"""
    try:
        if x is None:
            return float(default)
        if isinstance(x, (int, float)):
            return float(x)
        s = str(x).strip()
        if s == "":
            return float(default)
        return float(s)
    except Exception:
        return float(default)


def normalize_inventory_df(df: pd.DataFrame) -> pd.DataFrame:
    """確保庫存表欄位齊全、型別正確，移除多餘欄位（例如 label）"""
    df = df.copy()

    # 舊欄位轉換
    rename_map = {
        "庫存_原物料倉": "庫存_Wen",
        "庫存_半成品倉": "庫存_千畇",
        "庫存_成品倉": "庫存_James",
        "庫存_報廢倉": "庫存_Imeng",
    }
    df = df.rename(columns=rename_map)

    # 補欄位
    for col in INVENTORY_COLUMNS:
        if col not in df.columns:
            if col in ["總庫存", "均價"] or col.startswith("庫存_"):
                df[col] = 0.0
            else:
                df[col] = ""

    df = df[INVENTORY_COLUMNS].copy()

    # 轉型
    df["貨號"] = df["貨號"].astype(str)
    num_cols = ["總庫存", "均價"] + [f"庫存_{w}" for w in WAREHOUSES]
    for col in num_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

    return df


def normalize_history_df(df: pd.DataFrame) -> pd.DataFrame:
    """確保流水帳欄位齊全、型別正確"""
    df = df.copy()

    # 倉庫舊名稱轉換
    if "倉庫" in df.columns:
        replace_map = {
            "原物料倉": "Wen",
            "半成品倉": "千畇",
            "成品倉": "James",
            "報廢倉": "Imeng",
        }
        df["倉庫"] = df["倉庫"].replace(replace_map)

    # 補欄位
    for col in HISTORY_COLUMNS:
        if col not in df.columns:
            df[col] = 0 if col in NUM_HISTORY_COLS else ""

    df = df[HISTORY_COLUMNS].copy()

    # 數值欄位處理
    for col in NUM_HISTORY_COLS:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

    # 日期欄位改成字串避免 SQLite 解析錯誤
    if "日期" in df.columns:
        df["日期"] = df["日期"].astype(str)

    if "出貨日期" in df.columns:
        df["出貨日期"] = df["出貨日期"].astype(str)

    # 其他欄位統一字串
    for col in df.columns:
        if col not in NUM_HISTORY_COLS:
            df[col] = df[col].astype(str)

    return df
# =========================================================
# 2. SQLite + CSV 載入 / 儲存
# =========================================================

def load_from_db():
    """若 SQLite 存在則讀取，否則回傳 None"""
    if not os.path.exists(DB_FILE):
        return None, None
    try:
        conn = sqlite3.connect(DB_FILE)
        inv = pd.read_sql("SELECT * FROM inventory", conn)
        hist = pd.read_sql("SELECT * FROM history", conn)
        conn.close()
        return inv, hist
    except Exception:
        return None, None


def save_to_db(inv_df: pd.DataFrame, hist_df: pd.DataFrame):
    """同時寫入 SQLite（整表覆蓋）"""
    try:
        conn = sqlite3.connect(DB_FILE)
        inv_df.to_sql("inventory", conn, if_exists="replace", index=False)
        hist_df.to_sql("history", conn, if_exists="replace", index=False)
        conn.close()
    except Exception as e:
        st.warning(f"⚠️ 寫入 SQLite 失敗：{e}")


def load_data():
    """先從 SQLite 讀取，沒有則讀 CSV"""
    inv_df, hist_df = load_from_db()

    # --- 庫存 ---
    if inv_df is None:
        if os.path.exists(INVENTORY_CSV):
            try:
                inv_df = pd.read_csv(INVENTORY_CSV)
            except Exception:
                inv_df = pd.DataFrame(columns=INVENTORY_COLUMNS)
        else:
            inv_df = pd.DataFrame(columns=INVENTORY_COLUMNS)
    inv_df = normalize_inventory_df(inv_df)

    # --- 歷史 ---
    if hist_df is None:
        if os.path.exists(HISTORY_CSV):
            try:
                hist_df = pd.read_csv(HISTORY_CSV)
            except Exception:
                hist_df = pd.DataFrame(columns=HISTORY_COLUMNS)
        else:
            hist_df = pd.DataFrame(columns=HISTORY_COLUMNS)
    hist_df = normalize_history_df(hist_df)

    return inv_df, hist_df


def save_data():
    """同時保存 CSV + SQLite"""
    inv_df = normalize_inventory_df(st.session_state["inventory"])
    hist_df = normalize_history_df(st.session_state["history"])

    # CSV
    inv_df.to_csv(INVENTORY_CSV, index=False, encoding="utf-8-sig")
    hist_df.to_csv(HISTORY_CSV, index=False, encoding="utf-8-sig")

    # SQLite
    save_to_db(inv_df, hist_df)


# =========================================================
# 3. 篩選工具 + Excel 匯出
# =========================================================

def filter_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """通用篩選器 UI 元件"""
    if df is None or df.empty:
        return df

    modify = st.checkbox("🔍 開啟資料篩選器 (Filter Data)", key=f"filter_{id(df)}")
    if not modify:
        return df

    df = df.copy()

    # 嘗試解析日期欄位
    for col in df.columns:
        if is_object_dtype(df[col]):
            try:
                df[col] = pd.to_datetime(df[col])
            except Exception:
                pass

    with st.container():
        to_filter_columns = st.multiselect("選擇要篩選的欄位", df.columns)

        for column in to_filter_columns:
            left, right = st.columns((1, 20))
            left.write("↳")

            if is_categorical_dtype(df[column]) or df[column].nunique() < 50:
                options = sorted(df[column].astype(str).unique().tolist())
                use_all = right.checkbox(
                    f"全選 - {column}", value=True, key=f"chk_{column}"
                )
                if use_all:
                    filtered = options
                else:
                    filtered = right.multiselect(
                        f"選擇 {column} 的內容", options, default=[]
                    )

                df = df[df[column].astype(str).isin(filtered)]

            elif is_numeric_dtype(df[column]):
                min_v = float(df[column].min())
                max_v = float(df[column].max())
                step = (max_v - min_v) / 100 if max_v != min_v else 1
                val = right.slider(
                    f"設定 {column} 範圍", min_v, max_v, (min_v, max_v), step=step
                )
                df = df[df[column].between(*val)]

            elif is_datetime64_any_dtype(df[column]):
                dates = right.date_input(
                    f"選擇 {column} 的日期區間",
                    value=(df[column].min(), df[column].max()),
                )
                if len(dates) == 2:
                    start, end = map(pd.to_datetime, dates)
                    df = df[df[column].between(start, end)]

            else:
                txt = right.text_input(f"搜尋 {column} 包含的文字")
                if txt:
                    df = df[
                        df[column].astype(str).str.contains(txt, case=False, na=False)
                    ]

    return df


def convert_to_excel_all_sheets(inv_df, hist_df):
    """ 匯出多工作表 Excel """
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        inv_df.to_excel(writer, index=False, sheet_name="庫存總表")

        if hist_df is not None and not hist_df.empty:
            df = hist_df.copy()

            if "單據類型" in df.columns:
                df_in = df[df["單據類型"] == "進貨"]
                df_in.to_excel(writer, index=False, sheet_name="進貨紀錄")

                df_mfg = df[df["單據類型"].str.contains("製造", na=False)]
                df_mfg.to_excel(writer, index=False, sheet_name="製造紀錄")

                df_out = df[df["單據類型"].isin(["銷售出貨", "製造領料"])]
                df_out.to_excel(writer, index=False, sheet_name="出貨紀錄")

            df.to_excel(writer, index=False, sheet_name="完整流水帳")

    return output.getvalue()


def convert_single_sheet_to_excel(df, sheet_name="Sheet1"):
    """匯出單一工作表 Excel"""
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name=sheet_name)
    return output.getvalue()


# =========================================================
# 4. 庫存重算（最終穩定版）
# =========================================================

def recalculate_inventory(hist_df: pd.DataFrame, inv_df: pd.DataFrame):
    """加權平均法庫存重算（永不噴錯版本）"""

    hist_df = normalize_history_df(hist_df)
    inv_df = normalize_inventory_df(inv_df)

    # --- 補齊歷史中存在但庫存表不存在的 SKU ---
    hist_skus = set(hist_df["貨號"].astype(str))
    inv_skus = set(inv_df["貨號"].astype(str))
    new_skus = hist_skus - inv_skus

    if new_skus:
        missing = (
            hist_df[hist_df["貨號"].astype(str).isin(new_skus)][
                ["貨號", "系列", "分類", "品名"]
            ]
            .drop_duplicates("貨號")
            .copy()
        )
        for col in INVENTORY_COLUMNS:
            if col not in missing.columns:
                missing[col] = 0.0 if col in ["總庫存", "均價"] or col.startswith("庫存_") else ""
        inv_df = pd.concat([inv_df, missing[INVENTORY_COLUMNS]], ignore_index=True)

    # --- 重置庫存欄位 ---
    for col in ["總庫存", "均價"] + [f"庫存_{w}" for w in WAREHOUSES]:
        inv_df[col] = 0.0

    # --- 逐筆計算 ---
    for idx, row in inv_df.iterrows():
        sku = str(row["貨號"])
        target = hist_df[hist_df["貨號"].astype(str) == sku]

        total_qty = 0.0
        total_val = 0.0
        w_stock = {w: 0.0 for w in WAREHOUSES}

        for _, h in target.iterrows():
            qty = safe_float(h["數量"])
            cost = safe_float(h["進貨總成本"])
            t = h["單據類型"]
            wh = h["倉庫"] if h["倉庫"] in WAREHOUSES else WAREHOUSES[0]

            # 入庫類
            if t in ["進貨", "製造入庫", "調整入庫", "期初建檔", "庫存調整(加)"]:
                total_qty += qty
                if cost > 0:
                    total_val += cost
                w_stock[wh] += qty

            # 出庫類
            elif t in ["銷售出貨", "製造領料", "調整出庫", "庫存調整(減)"]:
                avg = (total_val / total_qty) if total_qty > 0 else 0
                total_qty -= qty
                total_val -= qty * avg
                if total_qty < 0:
                    total_qty = 0
                if total_val < 0:
                    total_val = 0
                w_stock[wh] -= qty

        inv_df.at[idx, "總庫存"] = total_qty
        inv_df.at[idx, "均價"] = (total_val / total_qty) if total_qty > 0 else 0

        for w in WAREHOUSES:
            inv_df.at[idx, f"庫存_{w}"] = w_stock[w]

    return normalize_inventory_df(inv_df)


# =========================================================
# 5. SKU / 批號 / 匯入工具
# =========================================================

def gen_batch_number(prefix="BAT"):
    return f"{prefix}-{datetime.now().strftime('%y%m%d%H%M')}"


def gen_mo_number():
    return f"MO-{datetime.now().strftime('%y%m%d-%H%M')}"


def get_dynamic_options(column_name, default_list):
    """讀取 inventory 中的選項 + 預設選項"""
    inv = st.session_state["inventory"]
    options = set(default_list)
    if column_name in inv.columns:
        for x in inv[column_name].dropna().unique():
            if str(x).strip():
                options.add(str(x))
    return sorted(options) + ["➕ 手動輸入新資料"]


def auto_generate_sku(category):
    prefix = PREFIX_MAP.get(category, "XX")
    df = st.session_state["inventory"]
    same = df[df["貨號"].str.startswith(prefix)]

    if same.empty:
        return f"{prefix}0001"

    try:
        max_num = (
            same["貨號"]
            .str.replace(prefix, "", regex=False)
            .str.extract(r"(\d+)")
            .iloc[:, 0]
            .astype(float)
            .max()
        )
        if pd.isna(max_num):
            return f"{prefix}0001"
        return f"{prefix}{int(max_num)+1:04d}"
    except:
        return f"{prefix}{int(time.time())}"


def process_product_upload(file_obj):
    """處理批次匯入商品資料"""
    try:
        df = pd.read_csv(file_obj) if file_obj.name.endswith(".csv") else pd.read_excel(file_obj)

        df = df.rename(
            columns={"名稱": "品名", "商品名稱": "品名", "類別": "分類", "SKU": "貨號"}
        )

        if "貨號" not in df.columns or "品名" not in df.columns:
            return None, "缺少必要欄位：貨號 / 品名"

        for col in ["系列", "分類"]:
            if col not in df.columns:
                df[col] = "未分類"

        new_df = df[["貨號", "系列", "分類", "品名"]].copy()
        new_df["貨號"] = new_df["貨號"].astype(str)
        return new_df, "OK"

    except Exception as e:
        return None, str(e)


def process_opening_stock_upload(file_obj, default_wh):
    """匯入期初庫存"""
    try:
        df = pd.read_csv(file_obj) if file_obj.name.endswith(".csv") else pd.read_excel(file_obj)

        df = df.rename(
            columns={
                "名稱": "品名",
                "商品名稱": "品名",
                "SKU": "貨號",
                "庫存": "數量",
                "現有庫存": "數量",
                "成本": "進貨總成本",
                "總成本": "進貨總成本",
            }
        )

        if "貨號" not in df.columns or "數量" not in df.columns:
            return None, "Excel 必須包含『貨號』『數量』欄位"

        inv = st.session_state["inventory"]

        results = []
        batch = f"INIT-{date.today():%Y%m%d}"

        for _, row in df.iterrows():
            sku = str(row["貨號"])
            qty = safe_float(row["數量"])
            if qty <= 0:
                continue

            wh = row["倉庫"] if ("倉庫" in df.columns and pd.notna(row["倉庫"])) else default_wh
            cost = safe_float(row.get("進貨總成本", 0))

            item = inv[inv["貨號"] == sku]
            if not item.empty:
                series = item.iloc[0]["系列"]
                cat = item.iloc[0]["分類"]
                name = item.iloc[0]["品名"]
            else:
                series = row.get("系列", "期初匯入")
                cat = row.get("分類", "期初匯入")
                name = row.get("品名", f"未知品名-{sku}")

            rec = {
                "單據類型": "期初建檔",
                "單號": f"OPEN-{int(time.time())}-{sku}",
                "日期": str(date.today()),
                "系列": series,
                "分類": cat,
                "品名": name,
                "貨號": sku,
                "批號": batch,
                "倉庫": wh,
                "數量": qty,
                "Key單者": "系統匯入",
                "進貨總成本": cost,
                "備註": "期初匯入",
            }

            for col in HISTORY_COLUMNS:
                if col not in rec:
                    rec[col] = 0 if col in NUM_HISTORY_COLS else ""

            results.append(rec)

        if not results:
            return pd.DataFrame(columns=HISTORY_COLUMNS), "OK"

        df_new = pd.DataFrame(results)
        return normalize_history_df(df_new), "OK"

    except Exception as e:
        return None, str(e)
# =========================================================
# 6. 初始化 Session State
# =========================================================

if "inventory" not in st.session_state or "history" not in st.session_state:
    inv, hist = load_data()
    st.session_state["inventory"] = inv
    st.session_state["history"] = hist

# =========================================================
# 7. 主介面
# =========================================================

st.title(f"🏭 {PAGE_TITLE}")

with st.sidebar:
    st.header("部門功能導航")
    page = st.radio(
        "選擇作業",
        [
            "📦 商品建檔與維護",
            "⚖️ 庫存盤點與調整",
            "📥 進貨庫存 (無金額)",
            "🔨 製造生產 (工廠)",
            "🚚 銷售出貨 (業務/出貨)",
            "📊 總表監控 (主管專用)",
            "💰 成本與財務管理 (加密)",
        ],
    )

    st.divider()
    st.markdown("### 💾 資料管理")

    if not st.session_state["history"].empty:
        with st.expander("📥 下載單獨報表", expanded=False):
            st.download_button(
                "📊 庫存現況表.xlsx",
                data=convert_single_sheet_to_excel(
                    normalize_inventory_df(st.session_state["inventory"]), "庫存表"
                ),
                file_name=f"Stock_{date.today()}.xlsx",
            )

            df_in = st.session_state["history"][
                st.session_state["history"]["單據類型"] == "進貨"
            ]
            st.download_button(
                "📥 進貨紀錄表.xlsx",
                data=convert_single_sheet_to_excel(df_in, "進貨紀錄"),
                file_name=f"Purchase_{date.today()}.xlsx",
            )

            df_out = st.session_state["history"][
                st.session_state["history"]["單據類型"].isin(["銷售出貨"])
            ]
            st.download_button(
                "🚚 銷貨紀錄表.xlsx",
                data=convert_single_sheet_to_excel(df_out, "銷貨紀錄"),
                file_name=f"Sales_{date.today()}.xlsx",
            )

            df_mfg = st.session_state["history"][
                st.session_state["history"]["單據類型"].str.contains("製造", na=False)
            ]
            st.download_button(
                "🔨 製造紀錄表.xlsx",
                data=convert_single_sheet_to_excel(df_mfg, "製造紀錄"),
                file_name=f"Mfg_{date.today()}.xlsx",
            )

        excel_data = convert_to_excel_all_sheets(
            normalize_inventory_df(st.session_state["inventory"]),
            st.session_state["history"],
        )
        st.download_button(
            label="📥 下載完整總表 (Excel)",
            data=excel_data,
            file_name=f"Report_Full_{date.today()}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

    with st.expander("⚙️ 系統還原 (上傳備份)", expanded=False):
        restore_file = st.file_uploader("上傳備份檔", type=["xlsx"], key="restore_file")
        if restore_file and st.button("確認還原並重算"):
            df_new_hist = process_restore_upload(restore_file)
            if df_new_hist is not None:
                st.session_state["history"] = df_new_hist
                st.session_state["inventory"] = recalculate_inventory(
                    df_new_hist, st.session_state["inventory"]
                )
                save_data()
                st.success("還原成功！")
                time.sleep(1)
                st.rerun()

# =========================================================
# 8. 各頁面功能
# =========================================================

# ---------------------------------------------------------
# 📦 商品建檔與維護
# ---------------------------------------------------------
if page == "📦 商品建檔與維護":
    st.subheader("📦 商品資料庫管理")

    tab_single, tab_batch, tab_opening, tab_list = st.tabs(
        ["✨ 單筆建檔", "📂 批次匯入", "📥 匯入期初庫存", "📋 檢視/修改"]
    )

    # === 單筆建檔 ===
    with tab_single:
        cat_opts = get_dynamic_options("分類", DEFAULT_CATEGORIES)
        cat_sel = st.selectbox("商品分類", cat_opts)
        final_cat = (
            st.text_input("↳ 輸入新分類名稱") if cat_sel == "➕ 手動輸入新資料" else cat_sel
        )

        ser_opts = get_dynamic_options("系列", DEFAULT_SERIES)
        ser_sel = st.selectbox("商品系列", ser_opts)
        final_ser = (
            st.text_input("↳ 輸入新系列名稱") if ser_sel == "➕ 手動輸入新資料" else ser_sel
        )

        name = st.text_input("商品品名")
        auto_sku = auto_generate_sku(final_cat)
        sku = st.text_input("商品貨號", value=auto_sku)

        if st.button("確認建立商品", type="primary"):
            if not name or not final_cat or not final_ser:
                st.error("品名、分類、系列 必填")
            else:
                df = st.session_state["inventory"]
                if sku in df["貨號"].values:
                    st.warning(f"⚠️ 貨號 {sku} 已存在")
                else:
                    new_row = {
                        "貨號": sku,
                        "系列": final_ser,
                        "分類": final_cat,
                        "品名": name,
                        "總庫存": 0.0,
                        "均價": 0.0,
                    }
                    for w in WAREHOUSES:
                        new_row[f"庫存_{w}"] = 0.0
                    df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
                    st.session_state["inventory"] = df
                    save_data()
                    st.success(f"已建立商品：{name} ({sku})")
                    time.sleep(1)
                    st.rerun()

    # === 批次匯入 (商品) ===
    with tab_batch:
        st.info("僅匯入商品資料，不改庫存。")
        up_prod = st.file_uploader("上傳 Excel / CSV", type=["xlsx", "csv"])
        if up_prod and st.button("開始匯入"):
            new_df, msg = process_product_upload(up_prod)
            if new_df is None:
                st.error(msg)
            else:
                df = st.session_state["inventory"]
                for _, row in new_df.iterrows():
                    sku = row["貨號"]
                    exists = df[df["貨號"] == sku]
                    if not exists.empty:
                        idx = exists.index[0]
                        df.at[idx, "品名"] = row["品名"]
                        df.at[idx, "分類"] = row["分類"]
                        df.at[idx, "系列"] = row["系列"]
                    else:
                        row_data = row.to_dict()
                        row_data["總庫存"] = 0
                        row_data["均價"] = 0
                        for w in WAREHOUSES:
                            row_data[f"庫存_{w}"] = 0
                        df = pd.concat([df, pd.DataFrame([row_data])], ignore_index=True)

                st.session_state["inventory"] = df
                save_data()
                st.success("匯入完成！")
                time.sleep(1)
                st.rerun()

    # === 匯入期初庫存 ===
    with tab_opening:
        target_wh = st.selectbox("無倉庫欄位時預設入庫至：", WAREHOUSES)
        up_file = st.file_uploader("上傳期初庫存 Excel/CSV", type=["xlsx", "csv"])
        if up_file and st.button("確認匯入庫存"):
            df_new, msg = process_opening_stock_upload(up_file, target_wh)
            if df_new is None:
                st.error(msg)
            elif df_new.empty:
                st.warning("無有效資料")
            else:
                st.session_state["history"] = pd.concat(
                    [st.session_state["history"], df_new], ignore_index=True
                )
                st.session_state["inventory"] = recalculate_inventory(
                    st.session_state["history"], st.session_state["inventory"]
                )
                save_data()
                st.success(f"成功匯入 {len(df_new)} 筆期初庫存！")
                time.sleep(1)
                st.rerun()

    # === 商品列表 ===
    with tab_list:
        df = get_safe_view(st.session_state["inventory"])
        df = filter_dataframe(df)

        edited = st.data_editor(
            df,
            num_rows="dynamic",
            use_container_width=True,
            column_config={
                "貨號": st.column_config.TextColumn(disabled=True),
                "總庫存": st.column_config.NumberColumn(disabled=True),
            },
        )

        if st.button("💾 儲存修改"):
            base = st.session_state["inventory"]
            for idx, row in edited.iterrows():
                if idx in base.index:
                    base.at[idx, "品名"] = row["品名"]
                    base.at[idx, "分類"] = row["分類"]
                    base.at[idx, "系列"] = row["系列"]
            st.session_state["inventory"] = base
            save_data()
            st.success("已更新！")


# ---------------------------------------------------------
# ⚖️ 庫存盤點與調整
# ---------------------------------------------------------
elif page == "⚖️ 庫存盤點與調整":
    st.subheader("⚖️ 庫存盤點與數量修正")

    inv = st.session_state["inventory"]
    if inv.empty:
        st.warning("請先建檔！")
    else:
        df = inv.copy()
        df["label"] = df["貨號"] + " | " + df["品名"]

        col1, col2 = st.columns([2, 1])
        with col1:
            sel_item = st.selectbox("選擇商品", df["label"])
            sku = df.loc[df["label"] == sel_item, "貨號"].iloc[0]
            row = inv[inv["貨號"] == sku].iloc[0]

        with col2:
            sel_wh = st.selectbox("調整倉庫", WAREHOUSES)

        curr_qty = safe_float(row[f"庫存_{sel_wh}"])
        st.metric("目前庫存", f"{int(curr_qty)}")

        with st.form("adj_form"):
            new_qty = st.number_input("盤點正確數量", min_value=0, value=int(curr_qty))
            reason = st.text_input("調整原因", value="盤點差異")

            if st.form_submit_button("確認修正"):
                diff = new_qty - curr_qty
                if diff == 0:
                    st.info("無變化")
                else:
                    t = "庫存調整(加)" if diff > 0 else "庫存調整(減)"
                    q = abs(diff)

                    rec = {
                        "單據類型": t,
                        "單號": f"ADJ-{int(time.time())}",
                        "日期": str(date.today()),
                        "系列": row["系列"],
                        "分類": row["分類"],
                        "品名": row["品名"],
                        "貨號": row["貨號"],
                        "批號": "",
                        "倉庫": sel_wh,
                        "數量": q,
                        "Key單者": "盤點",
                        "備註": reason,
                    }
                    for col in HISTORY_COLUMNS:
                        if col not in rec:
                            rec[col] = 0 if col in NUM_HISTORY_COLS else ""

                    st.session_state["history"] = pd.concat(
                        [st.session_state["history"], pd.DataFrame([rec])],
                        ignore_index=True,
                    )
                    st.session_state["inventory"] = recalculate_inventory(
                        st.session_state["history"], inv
                    )
                    save_data()
                    st.success(f"已更新庫存為 {new_qty}")
                    time.sleep(1)
                    st.rerun()


# ---------------------------------------------------------
# 📥 進貨庫存（無金額）
# ---------------------------------------------------------
elif page == "📥 進貨庫存 (無金額)":
    st.subheader("📥 無金額進貨")

    inv = st.session_state["inventory"]
    inv["label"] = inv["貨號"] + " | " + inv["品名"]

    with st.form("form_in"):
        col1, col2, col3 = st.columns([2, 1, 1])
        sel = col1.selectbox("選擇商品", inv["label"])
        sku = inv.loc[inv["label"] == sel, "貨號"].iloc[0]
        row = inv[inv["貨號"] == sku].iloc[0]

        wh = col2.selectbox("入庫倉庫", WAREHOUSES)
        qty = col3.number_input("進貨數量", min_value=1)

        c4, c5 = st.columns(2)
        dt = c4.date_input("進貨日期", date.today())
        user = c5.selectbox("Key單者", DEFAULT_KEYERS)

        vendor = st.text_input("廠商")
        note = st.text_input("備註")

        if st.form_submit_button("新增進貨", type="primary"):
            rec = {
                "單據類型": "進貨",
                "單號": datetime.now().strftime("%Y%m%d%H%M%S"),
                "日期": str(dt),
                "系列": row["系列"],
                "分類": row["分類"],
                "品名": row["品名"],
                "貨號": row["貨號"],
                "批號": gen_batch_number("IN"),
                "倉庫": wh,
                "數量": qty,
                "Key單者": user,
                "廠商": vendor,
                "備註": note,
                "進貨總成本": 0,
            }

            for col in HISTORY_COLUMNS:
                if col not in rec:
                    rec[col] = 0 if col in NUM_HISTORY_COLS else ""

            st.session_state["history"] = pd.concat(
                [st.session_state["history"], pd.DataFrame([rec])],
                ignore_index=True,
            )
            st.session_state["inventory"] = recalculate_inventory(
                st.session_state["history"], inv
            )
            save_data()
            st.success("進貨完成！")
            time.sleep(1)
            st.rerun()

    df = st.session_state["history"]
    df_view = df[df["單據類型"] == "進貨"]
    df_filtered = filter_dataframe(df_view)
    st.dataframe(df_filtered, use_container_width=True)


# ---------------------------------------------------------
# 🔨 製造生產
# ---------------------------------------------------------
elif page == "🔨 製造生產 (工廠)":
    st.subheader("🔨 製造生產紀錄")

    inv = st.session_state["inventory"]
    inv["label"] = inv["貨號"] + " | " + inv["品名"]

    tab1, tab2 = st.tabs(["📤 領料", "📥 完工"])


    # === 領料 ===
    with tab1:
        with st.form("mfg_out"):
            c1, c2 = st.columns(2)
            dt = c1.date_input("領料日期", value=date.today())
            mo = c2.text_input("工單單號", value=gen_mo_number())

            col1, col2 = st.columns([2, 1])
            sel = col1.selectbox("原料", inv["label"])
            sku = inv.loc[inv["label"] == sel, "貨號"].iloc[0]
            row = inv[inv["貨號"] == sku].iloc[0]
            wh = col2.selectbox("倉庫", WAREHOUSES)

            col3, col4 = st.columns(2)
            qty = col3.number_input("領料數量", min_value=1)
            user = col4.selectbox("領料人", DEFAULT_KEYERS)

            if st.form_submit_button("確認領料"):
                rec = {
                    "單據類型": "製造領料",
                    "單號": datetime.now().strftime("%Y%m%d%H%M%S"),
                    "日期": str(dt),
                    "系列": row["系列"],
                    "分類": row["分類"],
                    "品名": row["品名"],
                    "貨號": row["貨號"],
                    "批號": "",
                    "倉庫": wh,
                    "數量": qty,
                    "Key單者": user,
                    "訂單單號": mo,
                }
                for col in HISTORY_COLUMNS:
                    if col not in rec:
                        rec[col] = 0 if col in NUM_HISTORY_COLS else ""

                st.session_state["history"] = pd.concat(
                    [st.session_state["history"], pd.DataFrame([rec])],
                    ignore_index=True,
                )
                st.session_state["inventory"] = recalculate_inventory(
                    st.session_state["history"], inv
                )
                save_data()
                st.success("領料成功！")
                time.sleep(1)
                st.rerun()


    # === 完工 ===
    with tab2:
        with st.form("mfg_in"):
            c1, c2 = st.columns(2)
            dt = c1.date_input("完工日期", value=date.today())
            mo = c2.text_input("工單單號", value=gen_mo_number())

            col1, col2 = st.columns([2, 1])
            sel = col1.selectbox("成品", inv["label"])
            sku = inv.loc[inv["label"] == sel, "貨號"].iloc[0]
            row = inv[inv["貨號"] == sku].iloc[0]
            wh = col2.selectbox("入庫倉庫", WAREHOUSES)

            col3, col4, col5 = st.columns(3)
            qty = col3.number_input("產出量", min_value=1)
            batch = col4.text_input("批號", value=gen_batch_number("PD"))
            user = col5.selectbox("Key單者", DEFAULT_KEYERS)

            if st.form_submit_button("完工入庫"):
                rec = {
                    "單據類型": "製造入庫",
                    "單號": datetime.now().strftime("%Y%m%d%H%M%S"),
                    "日期": str(dt),
                    "系列": row["系列"],
                    "分類": row["分類"],
                    "品名": row["品名"],
                    "貨號": row["貨號"],
                    "批號": batch,
                    "倉庫": wh,
                    "數量": qty,
                    "Key單者": user,
                    "訂單單號": mo,
                }
                for col in HISTORY_COLUMNS:
                    if col not in rec:
                        rec[col] = 0 if col in NUM_HISTORY_COLS else ""

                st.session_state["history"] = pd.concat(
                    [st.session_state["history"], pd.DataFrame([rec])],
                    ignore_index=True,
                )
                st.session_state["inventory"] = recalculate_inventory(
                    st.session_state["history"], inv
                )
                save_data()
                st.success("完工入庫成功！")
                time.sleep(1)
                st.rerun()

    df = st.session_state["history"]
    df_view = df[df["單據類型"].str.contains("製造")]
    st.dataframe(filter_dataframe(df_view), use_container_width=True)


# ---------------------------------------------------------
# 🚚 銷售出貨
# ---------------------------------------------------------
elif page == "🚚 銷售出貨 (業務/出貨)":
    st.subheader("🚚 銷售出貨紀錄")

    inv = st.session_state["inventory"]
    inv["label"] = inv["貨號"] + " | " + inv["品名"]

    with st.form("sales_form"):
        col1, col2 = st.columns([2, 1])
        sel = col1.selectbox("商品", inv["label"])
        sku = inv.loc[inv["label"] == sel, "貨號"].iloc[0]
        row = inv[inv["貨號"] == sku].iloc[0]

        wh = col2.selectbox("出貨倉庫", WAREHOUSES)

        col3, col4, col5 = st.columns(3)
        qty = col3.number_input("數量", min_value=1)
        fee = col4.number_input("運費", min_value=0)
        dt = col5.date_input("出貨日期", date.today())

        c6, c7 = st.columns(2)
        ord_no = c6.text_input("訂單單號")
        user = c7.selectbox("Key單者", DEFAULT_KEYERS)

        note = st.text_area("備註")

        if st.form_submit_button("確認出貨", type="primary"):
            rec = {
                "單據類型": "銷售出貨",
                "單號": datetime.now().strftime("%Y%m%d%H%M%S"),
                "日期": str(dt),
                "系列": row["系列"],
                "分類": row["分類"],
                "品名": row["品名"],
                "貨號": row["貨號"],
                "批號": "",
                "倉庫": wh,
                "數量": qty,
                "運費": fee,
                "Key單者": user,
                "訂單單號": ord_no,
                "備註": note,
            }

            for col in HISTORY_COLUMNS:
                if col not in rec:
                    rec[col] = 0 if col in NUM_HISTORY_COLS else ""

            st.session_state["history"] = pd.concat(
                [st.session_state["history"], pd.DataFrame([rec])],
                ignore_index=True,
            )
            st.session_state["inventory"] = recalculate_inventory(
                st.session_state["history"], inv
            )
            save_data()
            st.success("出貨完成！")
            time.sleep(1)
            st.rerun()

    df = st.session_state["history"]
    df_filtered = df[df["單據類型"].isin(["銷售出貨", "製造領料"])]
    st.dataframe(filter_dataframe(df_filtered), use_container_width=True)


# ---------------------------------------------------------
# 📊 主管後台
# ---------------------------------------------------------
elif page == "📊 總表監控 (主管專用)":
    st.subheader("📊 主管後台（可編輯）")
    pwd = st.text_input("輸入主管密碼", type="password")
    if pwd == ADMIN_PASSWORD:
        st.success("登入成功！")

        tab_inv, tab_hist = st.tabs(["📦 庫存總表", "📜 流水帳"])

        # --- 可修正庫存資料 ---
        with tab_inv:
            df_inv = st.session_state["inventory"]
            df_inv_filtered = filter_dataframe(df_inv)

            edited = st.data_editor(
                df_inv_filtered,
                use_container_width=True,
                num_rows="dynamic",
                column_config={"總庫存": st.column_config.NumberColumn(disabled=True)},
            )

            if st.button("💾 儲存庫存修改"):
                st.session_state["inventory"] = normalize_inventory_df(edited)
                save_data()
                st.success("已更新庫存資料")

        # --- 可修正流水帳 ---
        with tab_hist:
            df_hist = st.session_state["history"]
            df_hist_filtered = filter_dataframe(df_hist)

            edited = st.data_editor(
                df_hist_filtered,
                use_container_width=True,
                num_rows="dynamic",
                height=600,
                column_config={
                    "倉庫": st.column_config.SelectboxColumn(options=WAREHOUSES),
                    "單據類型": st.column_config.SelectboxColumn(
                        options=[
                            "進貨",
                            "銷售出貨",
                            "製造領料",
                            "製造入庫",
                            "期初建檔",
                            "庫存調整(加)",
                            "庫存調整(減)",
                        ]
                    ),
                },
            )

            if st.button("💾 儲存修正並重算"):
                st.session_state["history"] = normalize_history_df(edited)
                st.session_state["inventory"] = recalculate_inventory(
                    st.session_state["history"],
                    st.session_state["inventory"],
                )
                save_data()
                st.success("已重新計算與保存")

    elif pwd != "":
        st.error("❌ 密碼錯誤")


# ---------------------------------------------------------
# 💰 財務（加密）
# ---------------------------------------------------------
elif page == "💰 成本與財務管理 (加密)":
    st.subheader("💰 成本中心")
    pwd = st.text_input("請輸入管理員密碼", type="password")

    if pwd == ADMIN_PASSWORD:
        st.success("登入成功")
        tab_fix, tab_full = st.tabs(["💸 補登進貨成本", "📜 流水帳（含金額）"])

        # === 進貨補登 ===
        with tab_fix:
            df = st.session_state["history"]
            df_fix = df[(df["單據類型"] == "進貨") & (df["進貨總成本"] == 0)]
            df_fix_filtered = filter_dataframe(df_fix)

            edited = st.data_editor(
                df_fix_filtered,
                column_config={
                    "進貨總成本": st.column_config.NumberColumn(required=True),
                },
            )

            if st.button("💾 儲存進貨成本"):
                df.update(edited)
                st.session_state["history"] = normalize_history_df(df)
                st.session_state["inventory"] = recalculate_inventory(
                    st.session_state["history"], st.session_state["inventory"]
                )
                save_data()
                st.success("已更新成本並重新計算庫存")

        # === 流水帳完整可編輯 ===
        with tab_full:
            df_all = st.session_state["history"]
            df_all_filtered = filter_dataframe(df_all)

            edited_all = st.data_editor(
                df_all_filtered,
                use_container_width=True,
                num_rows="dynamic",
            )

            if st.button("💾 儲存所有修正"):
                st.session_state["history"] = normalize_history_df(edited_all)
                st.session_state["inventory"] = recalculate_inventory(
                    st.session_state["history"], st.session_state["inventory"]
                )
                save_data()
                st.success("已更新並重新計算")

