import pandas as pd
from google.colab import drive


# ===============================================
# 基本讀取＋欄位清理
# ===============================================

def load_csv_and_record_rows(file_path):
    """讀取 CSV、標準化欄位名稱、印出基本資訊"""
    drive.mount('/drive', force_remount=True)

    print(f"\n📂 正在處理檔案: {file_path}")
    df = pd.read_csv(file_path, low_memory=False)

    # 標準化欄名
    df.columns = [col.lower().strip() for col in df.columns]

    rename_dict = {'public_date': 'date', 'datadate': 'date'}
    df.rename(columns={c: rename_dict[c] for c in df.columns if c in rename_dict}, inplace=True)

    # 修正 id 欄位
    for col in df.columns:
        if 'gvkey' in col:
            df.rename(columns={col: 'gvkey'}, inplace=True)
        if 'permno' in col:
            df.rename(columns={col: 'permno'}, inplace=True)

    print(f"➡ 原始行數: {df.shape[0]}")
    print(f"➡ 欄位數量: {len(df.columns)}")
    print(f"➡ 欄位: {list(df.columns)}")

    # 印出唯一識別ID
    if 'permno' in df.columns:
        print("唯一 permno:", df['permno'].nunique())
    if 'gvkey' in df.columns:
        print("唯一 gvkey:", df['gvkey'].nunique())

    # 印出日期範圍
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'], errors='coerce')
        print("📅 日期範圍:", df['date'].min(), "~", df['date'].max())

    return df


# ===============================================
# 工具函數：統計每月筆數
# ===============================================

def calculate_monthly_data_counts(df):
    df['date'] = pd.to_datetime(df['date'])
    result = df.assign(year_month=df['date'].dt.to_period('M'))['year_month'].value_counts().sort_index()
    return result.reset_index().rename(columns={'index': 'year_month', 'year_month': 'count'})


# ===============================================
# Step 1：修正負股價
# ===============================================

def convert_prc_to_positive(df):
    df['prc'] = df['prc'].abs()
    return df


# ===============================================
# Step 2：PRC 成長率＋向上 shift
# ===============================================

def calculate_prc_growth_and_shift(df):
    df['date'] = pd.to_datetime(df['date'])
    df.sort_values(['permno', 'ncusip', 'date'], inplace=True)

    periods = [1, 3, 6, 9, 12]

    # 計算 pct_change
    for p in periods:
        df[f'PRC GROWTH {p}m'] = df.groupby(['permno', 'ncusip'])['prc'].pct_change(p)

    # shift upward
    def shift_up(group):
        for p in periods:
            col = f'PRC GROWTH {p}m'
            group[col] = group[col].shift(-p)
        return group

    df = df.groupby(['permno', 'ncusip'], group_keys=False).apply(shift_up)

    # 移除全為 NaN
    df.dropna(subset=[f'PRC GROWTH {p}m' for p in periods], how='all', inplace=True)

    return df


# ===============================================
# Step 3：輸出成長率檔案
# ===============================================

def export_growth_files(df):
    periods = [1, 3, 6, 9, 12]

    for p in periods:
        col = f'PRC GROWTH {p}m'
        output = df.drop(columns=[c for c in df.columns if c.startswith("PRC GROWTH") and c != col])
        out_path = f'/drive/MyDrive/論文/data/final_result_{p}m.csv'
        output.to_csv(out_path, index=False)
        print(f"✔ 成長率 {p}m 已輸出 → {out_path}")


# ===============================================
# Step 4：檢查每個 permno/ncusip 是否連續（月資料）
# ===============================================

def process_and_check_continuity(period):
    path = f'/drive/MyDrive/論文/data/final_result_{period}m.csv'
    df = pd.read_csv(path)
    df['date'] = pd.to_datetime(df['date'])

    col = f'PRC GROWTH {period}m'
    df = df.dropna(subset=[col])

    continuous, non_continuous = [], []

    for (_, _), g in df.groupby(['permno', 'ncusip']):
        g = g.sort_values('date')
        ym = g['date'].dt.to_period('M')
        diff = ym.diff().dropna()

        if (diff == 1).all():
            continuous.append(g)
        else:
            non_continuous.append(g)

    df_cont = pd.concat(continuous)
    df_non = pd.concat(non_continuous)

    df_cont.to_csv(path, index=False)
    df_non.to_csv(f'/drive/MyDrive/論文/data/processed_result_{period}m_non_continuous.csv', index=False)

    print(f"✔ {period}m：不連續 {len(non_continuous)} 組已輸出")
    return df_cont, df_non


def process_all_periods():
    results = {}
    for p in [1, 3, 6, 9, 12]:
        results[p] = process_and_check_continuity(p)
    return results


# ===============================================
# Step 5：標準差計算 + shift
# ===============================================

def calculate_std_dev_and_shift(df):
    df['date'] = pd.to_datetime(df['date'])
    df.sort_values(['permno', 'ncusip', 'date'], inplace=True)

    periods = [1, 3, 6, 9, 12]

    # rolling std
    for p in periods:
        col = f'PRC STD {p}m'
        df[col] = df.groupby(['permno', 'ncusip'])['prc'].transform(lambda x: x.rolling(p, min_periods=1).std())

    # shift upward
    for p in periods:
        col = f'PRC STD {p}m'
        df[col] = df.groupby(['permno', 'ncusip'])[col].shift(-p)

    df.dropna(subset=[f'PRC STD {p}m' for p in periods], inplace=True)

    # 輸出
    for p in periods:
        col = f'PRC STD {p}m'
        out = df.drop(columns=[c for c in df.columns if c.startswith("PRC STD") and c != col])
        path = f'/drive/MyDrive/論文/data/final_result_{p}m_std.csv'
        out.to_csv(path, index=False)
        print(f"✔ 標準差 {p}m 已輸出 → {path}")

    return df


# ===============================================
# Step 6：移除最後 n 個月資料
# ===============================================

def remove_last_n_months(period):
    path = f'/drive/MyDrive/論文/data/final_result_{period}m.csv'
    df = pd.read_csv(path)
    df['date'] = pd.to_datetime(df['date'])

    unique_dates = df['date'].sort_values().unique()
    if len(unique_dates) > period:
        cutoff = unique_dates[-period]
        df = df[df['date'] < cutoff]

    df.to_csv(path, index=False)
    print(f"✔ 已刪除最後 {period} 個月 → {path}")
    return df


# ===============================================
# 🚀 主流程開始
# ===============================================

print("\n===== STEP 1: Load Data =====")
df = load_csv_and_record_rows('/drive/MyDrive/論文/data/data_final.csv')

print("\n===== STEP 2: Convert PRC Negative =====")
df = convert_prc_to_positive(df)

print("\n===== STEP 3: Calculate PRC Growth =====")
df_growth = calculate_prc_growth_and_shift(df)

print("\n===== STEP 4: Export Growth Files =====")
export_growth_files(df_growth)

print("\n===== STEP 5: Check Continuity =====")
results = process_all_periods()

print("\n===== STEP 6: Monthly Summary =====")
for p in [1, 3, 6, 9, 12]:
    summary = calculate_monthly_data_counts(pd.read_csv(f'/drive/MyDrive/論文/data/final_result_{p}m.csv'))
    print(f"\n📊 {p}m monthly counts")
    print(summary)

print("\n===== STEP 7: Remove Last N Months =====")
for p in [1, 3, 6, 9, 12]:
    remove_last_n_months(p)

print("\n🎉 完成：所有功能整合成功（功能完全保留）")
