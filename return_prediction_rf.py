import os
import random
import numpy as np
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import mean_squared_error, mean_absolute_error
from dateutil.relativedelta import relativedelta
import time
import psutil
from google.colab import drive

drive.mount('/drive', force_remount=True)

# 設定隨機種子，確保模型結果可重現
def set_random_seed(seed):
    os.environ['PYTHONHASHSEED'] = str(seed)
    random.seed(seed)
    np.random.seed(seed)

# 訓練 + 預測函式
def train_and_predict(data, start_date, end_date, prediction_date, growth_period, seed=42):
    set_random_seed(seed)

    # 擷取訓練視窗資料
    window_data = data.loc[start_date:end_date]
    if window_data.empty:
        print(f"⚠️ 區間無資料：{start_date} ~ {end_date}")
        return None, None, None, None, None

    # 取 permno 與 ncusip 做標識
    stock_ids = window_data[['permno', 'ncusip']].copy()

    # 特徵與目標值
    X = window_data[['bm', 'pe_exi', 'pe_inc', 'ptb', 'gprof', 'gpm',
                     'npm', 'opmad', 'roa', 'roe', 'cfm', 'cash_debt',
                     'short_debt', 'curr_debt', 'de_ratio', 'debt_at',
                     'quick_ratio', 'curr_ratio', 'rect_turn', 'at_turn', 'rd_sale']]

    y = window_data[f'PRC GROWTH {growth_period}m']

    # 拆分訓練 / 驗證資料
    X_train, X_val, y_train, y_val = train_test_split(
        X, y, test_size=0.1, random_state=seed
    )

    # 標準化特徵
    sc = StandardScaler()
    X_train = sc.fit_transform(X_train)
    X_val = sc.transform(X_val)

    # 建立隨機森林模型
    model = RandomForestRegressor(n_estimators=5, random_state=seed)

    # 訓練模型
    start_time = time.time()
    model.fit(X_train, y_train)
    end_time = time.time()
    training_time = end_time - start_time

    # 驗證模型
    val_predictions = model.predict(X_val)
    val_rmse = np.sqrt(mean_squared_error(y_val, val_predictions))
    val_mae = mean_absolute_error(y_val, val_predictions)
    print(f"📊 驗證 RMSE: {val_rmse}, MAE: {val_mae}")

    # 準備預測資料
    prediction_data = data[data.index == prediction_date]
    if prediction_data.empty:
        print(f"⚠️ 無預測資料：{prediction_date}")
        return None, None, None, None, None

    X_pred = prediction_data[['bm', 'pe_exi', 'pe_inc', 'ptb', 'gprof', 'gpm',
                              'npm', 'opmad', 'roa', 'roe', 'cfm', 'cash_debt',
                              'short_debt', 'curr_debt', 'de_ratio', 'debt_at',
                              'quick_ratio', 'curr_ratio', 'rect_turn', 'at_turn', 'rd_sale']]

    X_pred = sc.transform(X_pred)

    # 預測未來成長率
    y_pred = model.predict(X_pred)

    return y_pred, prediction_data[f'PRC GROWTH {growth_period}m'], stock_ids, X_pred, training_time


# 主流程：處理不同視窗與不同預測年期
def process_all_windows_and_years(base_path):
    growth_periods = [12]  # 預測 12 個月報酬
    prediction_years = [4, 5]  # 預測 horizon 為 4 年與 5 年

    for growth_period in growth_periods:
        dataset_path = f'/drive/MyDrive/論文/data/final_result_{growth_period}m.csv'

        if os.path.exists(dataset_path):

            # 載入資料
            dataset = pd.read_csv(dataset_path)
            dataset['date'] = pd.to_datetime(dataset['date'])
            dataset = dataset.sort_values('date')
            dataset.set_index('date', inplace=True)

            for years in prediction_years:

                # 找出資料最早日期
                earliest_date = dataset.index.min()
                print(f"📅 資料最早日期：{earliest_date}")

                # 設定第一個預測日期
                prediction_date = earliest_date + relativedelta(years=years)

                all_results = []
                total_training_time = 0
                start_memory = psutil.virtual_memory().used

                # 滾動視窗逐月預測
                while prediction_date <= dataset.index.max():

                    start_date = prediction_date - relativedelta(years=years) + relativedelta(days=1)
                    end_date = prediction_date - relativedelta(days=1)

                    y_pred, y_true, stock_ids, X_pred, training_time = \
                        train_and_predict(dataset, start_date, end_date, prediction_date, growth_period)

                    total_training_time += training_time

                    if y_pred is not None and y_true is not None:

                        results_df = pd.DataFrame({
                            'permno': stock_ids['permno'].values,
                            'ncusip': stock_ids['ncusip'].values,
                            'Date': [prediction_date] * len(y_pred),
                            'True Values': y_true.values,
                            'Predicted Values': y_pred,
                            'Window Start': [start_date] * len(y_pred),
                            'Window End': [end_date] * len(y_pred)
                        })

                        all_results.append(results_df)
                        print(f"✅ 完成預測：{prediction_date}")

                    # 預測日期往後推一個月
                    prediction_date += relativedelta(months=1)

                # 計算記憶體使用量
                end_memory = psutil.virtual_memory().used

                # 合併所有預測結果
                final_results_df = pd.concat(all_results)

                # 計算整體 RMSE 與 MAE
                overall_rmse = np.sqrt(mean_squared_error(
                    final_results_df['True Values'],
                    final_results_df['Predicted Values']
                ))
                overall_mae = mean_absolute_error(
                    final_results_df['True Values'],
                    final_results_df['Predicted Values']
                )

                print(f"📉 全期間 RMSE：{overall_rmse}")
                print(f"📉 全期間 MAE：{overall_mae}")
                print(f"⏱️ 總訓練時間：{total_training_time} 秒")
                print(f"🧠 記憶體使用量：{end_memory - start_memory} bytes")

                # 輸出結果
                final_results_filename = f'/drive/MyDrive/論文/data/RF_{growth_period}M_{years}y_predictions_vs_true_val_adjust.csv'
                final_results_df.to_csv(final_results_filename, index=False)
                print(f"📁 結果已儲存：{final_results_filename}")


# 執行主流程
process_all_windows_and_years('/drive/MyDrive/論文/data/')

