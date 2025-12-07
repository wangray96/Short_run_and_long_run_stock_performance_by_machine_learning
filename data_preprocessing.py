import pandas as pd
from google.colab import drive

# ============================================================
# 0. 基礎設定
# ============================================================
pd.set_option("display.max_columns", None)
drive.mount('/drive', force_remount=True)


# ============================================================
# 1. 通用工具函式區
# ============================================================

def load_csv(file_path, encoding="utf-8"):
    """讀取 CSV + 標準化欄位名稱 + 回傳 DataFrame"""
    print(f"\n📂 Loading file: {file_path}")

    try:
        df = pd.read_csv(file_path, encoding=encoding, on_bad_lines="skip", low_memory=False)
    except UnicodeDecodeError:
        print("⚠️ UTF-8 解碼失敗，改用 latin1")
        df = pd.read_csv(file_path, encoding="latin1", on_bad_lines="skip", low_memory=False)

    # 標準化欄位名稱
    df.columns = [col.lower().strip() for col in df.columns]

    rename_dict = {"public_date": "date", "datadate": "date"}
    df.rename(columns={c: rename_dict[c] for c in df.columns if c in rename_dict}, inplace=True)

    # gvkey / permno 自動標準化
    for col in df.columns:
        if "gvkey" in col:
            df.rename(columns={col: "gvkey"}, inplace=True)
        if "permno" in col:
            df.rename(columns={col: "permno"}, inplace=True)

    # 日期格式處理
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")

    print(f"✔ rows: {len(df)}, columns: {len(df.columns)}")
    return df


# ============================================================
# 2. 資料統計函式
# ============================================================

def summarize_columns(df, cols):
    """統計指定欄位的缺失值比例"""
    print("\n📊 Column summary:")
    for col in cols:
        if col not in df.columns:
            continue
        miss = df[col].isna().sum()
        total = len(df)
        print(f"{col}: missing {miss} ({miss/total:.2%})")


# ============================================================
# 3. 分組排序
# ============================================================

def sort_by_group(df, date_col="date"):
    """依照 gvkey 或 permno 分組並排序"""
    key = "gvkey" if "gvkey" in df.columns else "permno"
    df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
    return df.sort_values([key, date_col])


# ============================================================
# 4. 移除重複資料
# ============================================================

def remove_duplicate_permno_date(df, output_path):
    """刪除 permno+date 重複資料，輸出被刪除的資料"""
    print("\n🧹 Removing duplicate (permno, date) rows...")

    before = len(df)
    dup = df[df.duplicated(subset=["permno", "date"], keep=False)]
    dup.to_csv(output_path, index=False)

    df_clean = df.drop_duplicates(subset=["permno", "date"], keep="first")

    print(f"✔ Before: {before}, After: {len(df_clean)}, Removed: {len(dup)}")
    return df_clean


# ============================================================
# 5. 檢查每個公司資料是否為「連續月份」
# ============================================================

def extract_continuous_monthly(df, id_col, delete_file, missing_file):
    """拆分連續 vs 不連續月份資料，並輸出不連續部分 + 缺失月份報告"""
    print(f"\n📅 Checking monthly continuity for {id_col}...")

    df["date"] = pd.to_datetime(df["date"])
    continuous = []
    removed = []
    missing_records = []

    for id_value, group in df.groupby(id_col):
        group = group.sort_values("date")
        months = pd.date_range(group["date"].min(), group["date"].max(), freq="MS")
        missing = months.difference(group["date"].dt.to_period("M").dt.to_timestamp())

        if len(missing) == 0:
            continuous.append(group)
        else:
            removed.append(group)
            for m in missing:
                missing_records.append([id_value, m])

    removed_df = pd.concat(removed) if removed else pd.DataFrame()
    continuous_df = pd.concat(continuous) if continuous else pd.DataFrame()

    removed_df.to_csv(delete_file, index=False)
    pd.DataFrame(missing_records, columns=[id_col, "missing_date"]).to_csv(missing_file, index=False)

    print(f"✔ Continuous: {len(continuous_df)}, Removed: {len(removed_df)}")
    return continuous_df


# ============================================================
# 6. 合併 CRSP + IBES
# ============================================================

def merge_crsp_ibes(crsp, ibes):
    """以 permno + 月份 合併 CRSP 與 IBES"""
    crsp["date"] = crsp["date"].dt.to_period("M")
    ibes["date"] = ibes["date"].dt.to_period("M")

    merged = pd.merge(crsp, ibes, on=["permno", "date"], how="inner")
    merged["date"] = merged["date"].dt.to_timestamp()

    print(f"\n🔗 Merge result: rows={len(merged)}, permno={merged['permno'].nunique()}")
    return merged


# ============================================================
# 7. 主流程
# ============================================================

# 路徑（保持與你原本一致）
IBES_raw = "/drive/MyDrive/論文/data/financial_ratio_all_IBES.csv"
CRSP_raw = "/drive/MyDrive/論文/data/CRSP_Stock_price_Monthly_final.csv"

IBES_out = "/drive/MyDrive/論文/data/output_IBES.csv"
CRSP_out = "/drive/MyDrive/論文/data/output_crsp.csv"

dup_IBES = "/drive/MyDrive/論文/data/data_duplicate.csv"
dup_CRSP = "/drive/MyDrive/論文/data/price_duplicate.csv"

noncon_IBES = "/drive/MyDrive/論文/data/non_continuous_data1.csv"
noncon_IBES_dates = "/drive/MyDrive/論文/data/non_continuous_date1.csv"

noncon_CRSP = "/drive/MyDrive/論文/data/non_continuous_data2.csv"
noncon_CRSP_dates = "/drive/MyDrive/論文/data/non_continuous_date2.csv"

merged_delete = "/drive/MyDrive/論文/merged_data_delete.csv"
merged_missing = "/drive/MyDrive/論文/merged_data_dates.csv"

merged_final_path = "/drive/MyDrive/論文/data/merged_data_final.csv"


# -------------------------
# Step 1: 載入資料
# -------------------------
crsp = load_csv(CRSP_raw)
ibes = load_csv(IBES_raw)

# -------------------------
# Step 2: 排序
# -------------------------
crsp = sort_by_group(crsp)
ibes = sort_by_group(ibes)

# -------------------------
# Step 3: 移除重複資料
# -------------------------
crsp = remove_duplicate_permno_date(crsp, dup_CRSP)
ibes = ibes  # IBES 以 gvkey 為主，不做 duplicate 清理

# -------------------------
# Step 4: 找出連續月份資料
# -------------------------
ibes_clean = extract_continuous_monthly(ibes, "gvkey", noncon_IBES, noncon_IBES_dates)
crsp_clean = extract_continuous_monthly(crsp, "permno", noncon_CRSP, noncon_CRSP_dates)

# -------------------------
# Step 5: 合併資料
# -------------------------
merged = merge_crsp_ibes(crsp_clean, ibes_clean)

# -------------------------
# Step 6: 輸出結果
# -------------------------
merged.to_csv(merged_final_path, index=False)
print("\n🎉 All processes completed! File saved:", merged_final_path)
