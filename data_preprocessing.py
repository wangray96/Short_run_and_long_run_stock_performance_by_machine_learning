import pandas as pd
from google.colab import drive

# ============================================================
# 0. 基礎設定
# ============================================================
pd.set_option("display.max_columns", None)
drive.mount('/drive', force_remount=True)


# ============================================================
# 1. 通用工具：載入 CSV、標準化欄位
# ============================================================

def load_csv(file_path, encoding="utf-8"):
    """讀取 CSV + 標準化欄位名稱 + 回傳 DataFrame"""
    print(f"\n📂 Loading file: {file_path}")

    try:
        df = pd.read_csv(file_path, encoding=encoding, on_bad_lines="skip", low_memory=False)
    except UnicodeDecodeError:
        print("⚠️ UTF-8 解碼失敗，改用 latin1")
        df = pd.read_csv(file_path, encoding="latin1", on_bad_lines="skip", low_memory=False)

    # 標準化欄位
    df.columns = [col.lower().strip() for col in df.columns]

    rename_dict = {"public_date": "date", "datadate": "date"}
    df.rename(columns={c: rename_dict[c] for c in df.columns if c in rename_dict}, inplace=True)

    # gvkey / permno 自動標準化
    for col in df.columns:
        if "gvkey" in col:
            df.rename(columns={col: "gvkey"}, inplace=True)
        if "permno" in col:
            df.rename(columns={col: "permno"}, inplace=True)

    # 日期格式
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")

    print(f"✔ rows: {len(df)}, columns: {len(df.columns)}")
    return df


# ============================================================
# 2. 分組排序
# ============================================================

def sort_by_group(df, date_col="date"):
    """依照 gvkey 或 permno 分組並排序"""
    key = "gvkey" if "gvkey" in df.columns else "permno"
    return df.sort_values([key, date_col])


# ============================================================
# 3. 移除重複資料（CRSP）
# ============================================================

def remove_duplicate_permno_date(df, output_path):
    """刪除 permno+date 重複資料，輸出被刪除資料"""
    print("\n🧹 Removing duplicate (permno, date) rows...")

    before = len(df)
    dup = df[df.duplicated(subset=["permno", "date"], keep=False)]
    dup.to_csv(output_path, index=False)

    df_clean = df.drop_duplicates(subset=["permno", "date"], keep="first")

    print(f"✔ Before: {before}, After: {len(df_clean)}, Removed: {len(dup)}")
    return df_clean


# ============================================================
# 4. 找出連續月份資料
# ============================================================

def extract_continuous_monthly(df, id_col, delete_file, missing_file):
    """拆分連續 vs 不連續月份資料，並輸出不連續部分 + 缺失月份"""
    print(f"\n📅 Checking monthly continuity for {id_col}...")

    df["date"] = pd.to_datetime(df["date"])
    continuous, removed, missing_records = [], [], []

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

    print(f"✔ Continuous groups: {len(continuous_df)}, Removed groups: {len(removed_df)}")
    return continuous_df


# ============================================================
# 5. 合併 CRSP × IBES
# ============================================================

def merge_crsp_ibes(crsp, ibes):
    """以 permno + 月份合併"""
    crsp["date"] = crsp["date"].dt.to_period("M")
    ibes["date"] = ibes["date"].dt.to_period("M")

    merged = pd.merge(crsp, ibes, on=["permno", "date"], how="inner")
    merged["date"] = merged["date"].dt.to_timestamp()

    print(f"\n🔗 Merge done → rows={len(merged)}, permno={merged['permno'].nunique()}")
    return merged


# ============================================================
# 6. Cut-off（刪除 1970 年以前資料）
# ============================================================

def remove_data_before_year(data, date_column, cutoff_year):
    """刪除某年份以前的資料"""
    data[date_column] = pd.to_datetime(data[date_column], errors="coerce")
    data["year"] = data[date_column].dt.year

    before = len(data)
    data = data[data["year"] > cutoff_year].drop(columns=["year"])
    after = len(data)

    print(f"\n⛔ Cut-off applied: removed {before - after} rows ≤ {cutoff_year}")
    return data


# ============================================================
# 7. 缺失值檢查與刪除不良股票
# ============================================================

def preprocess_data(df, columns_to_check):
    print(f"\n📌 Preprocess → 原始筆數: {df.shape[0]}")

    deleted = []
    drop_list = []

    for (permno, ncusip), group in df.groupby(["permno", "ncusip"]):

        for col in columns_to_check:
            if col not in df.columns:
                continue

            # 連續 ≥ 8 個 NA → 刪除該股票
            consec_na = (
                group[col].isna()
                .astype(int)
                .groupby(group[col].notna().astype(int).cumsum())
                .sum()
                .max()
            )

            if consec_na >= 8:
                drop_list.append((permno, ncusip))
                deleted.append(group)
                break

    mask = df.set_index(["permno", "ncusip"]).index.isin(drop_list)
    df_clean = df[~mask]

    print(f"✔ Removed {len(drop_list)} bad permno/ncusip groups")

    pd.concat(deleted).to_csv("/drive/MyDrive/論文/data/deleted_groups.csv", index=False)

    return df_clean


# ============================================================
# 8. 填補缺失值（前後填補）
# ============================================================

def fill_missing_values(df, cols):
    print("\n🧩 Filling missing values...")

    for col in cols:
        if col in df.columns:
            df[col] = df.groupby(["permno", "ncusip"])[col].apply(
                lambda x: x.ffill().bfill()
            )

    print("✔ Missing values filled.")
    return df


# ============================================================
# 9. 主流程執行
# ============================================================

IBES_raw = "/drive/MyDrive/論文/data/financial_ratio_all_IBES.csv"
CRSP_raw = "/drive/MyDrive/論文/data/CRSP_Stock_price_Monthly_final.csv"

dup_CRSP = "/drive/MyDrive/論文/data/price_duplicate.csv"
noncon_IBES = "/drive/MyDrive/論文/data/non_continuous_data1.csv"
noncon_IBES_dates = "/drive/MyDrive/論文/data/non_continuous_date1.csv"
noncon_CRSP = "/drive/MyDrive/論文/data/non_continuous_data2.csv"
noncon_CRSP_dates = "/drive/MyDrive/論文/data/non_continuous_date2.csv"

merged_final_path = "/drive/MyDrive/論文/data/merged_data_final.csv"

# Step 1: Load
crsp = load_csv(CRSP_raw)
ibes = load_csv(IBES_raw)

# Step 2: Sort
crsp = sort_by_group(crsp)
ibes = sort_by_group(ibes)

# Step 3: Remove duplicate rows (CRSP only)
crsp = remove_duplicate_permno_date(crsp, dup_CRSP)

# Step 4: Keep only continuous monthly data
ibes_clean = extract_continuous_monthly(ibes, "gvkey", noncon_IBES, noncon_IBES_dates)
crsp_clean = extract_continuous_monthly(crsp, "permno", noncon_CRSP, noncon_CRSP_dates)

# Step 5: Merge CRSP × IBES
merged = merge_crsp_ibes(crsp_clean, ibes_clean)

# ⭐ Step 6: Remove data ≤ 1970
merged = remove_data_before_year(merged, "date", 1970)

# Step 7: Preprocess — remove bad stocks
cols_to_check = [
    "bm","evm","pe_exi","pe_inc","ptb","gprof","gpm","npm",
    "opmad","roa","roe","cfm","cash_debt","short_debt",
    "curr_debt","de_ratio","debt_at","quick_ratio",
    "curr_ratio","rect_turn","at_turn","rd_sale","prc"
]
merged = preprocess_data(merged, cols_to_check)

# Step 8: Fill missing values
merged = fill_missing_values(merged, cols_to_check)

# Step 9: Save result
merged.to_csv(merged_final_path, index=False)
print("\n🎉 完成！Final dataset saved:", merged_final_path)

