"""
データを更新するためのスクリプト
"""
import math
import sqlite3
import time
import urllib.request
from datetime import datetime

import pandas as pd
from bs4 import BeautifulSoup
from tqdm import tqdm


def load_codes_dataframe():
    """
    銘柄コードのデータフレームを取得する
    """
    query = 'select code from Symbols;'
    conn = sqlite3.connect('stocks.db')
    with conn:
        df = pd.read_sql_query(query, conn)

    return df


def delete_code(code):
    """
    上場廃止された銘柄をリストから削除する
    """
    query = f'delete from Symbols where code is \"{code}\";'
    conn = sqlite3.connect('stocks.db')
    with conn:
        cur = conn.cursor()
        cur.execute(query)
        conn.commit()


def obtain_num_days(code):
    """
    株価のページから最新の日付を取得し、データベースに格納されている最新の日付との差分を計算する
    """

    # データベースに格納されている最新の日付を取得する
    query = f'select distinct date from \"{code}\" order by date desc limit 1;'
    conn = sqlite3.connect('stocks.db')
    with conn:
        df = pd.read_sql_query(query, conn)
    last_date = df['date'].values[0]
    last_date_dt = datetime.strptime(last_date, '%Y-%m-%d')
    last_date_1 = last_date_dt.date()

    # 株価のページから最新の日付を取得する
    url = f'https://kabutan.jp/stock/kabuka?code={code}&page=1'
    with urllib.request.urlopen(url) as res:
        soup = BeautifulSoup(res, 'html.parser')
    try:
        last_date = soup.find("table", class_="stock_kabuka0").find("time")[
            "datetime"]
    except AttributeError:
        return -1
    last_date_dt = datetime.strptime(last_date, '%Y-%m-%d')
    last_date_2 = last_date_dt.date()

    # 差分日数を計算する
    num_days = (last_date_2 - last_date_1).days

    return num_days


def fetch_values_dataframe(code, i):
    """
    株価のページからデータフレームを取得する
    """
    df = pd.DataFrame([], columns=['open', 'high', 'low', 'close', 'volume'])

    url = f'https://kabutan.jp/stock/kabuka?code={code}&page={i}'
    with urllib.request.urlopen(url) as res:
        soup = BeautifulSoup(res, 'html.parser')

    idx = 0 if i == 1 else 1

    # 日付を取得
    v_date = soup.find_all("th", scope="row")
    v_date_list = [
        v_date[i].find("time")["datetime"] for i in range(4 + idx, len(v_date))
    ]
    v_date_df = pd.DataFrame({'date': v_date_list})

    # 株価を取得
    v_values = soup.find_all(id="stock_kabuka_table")[0].find_all("td")
    v_values_list = []
    for j in range(0 + idx * 7, len(v_values), 7):
        v_open = v_values[j].text
        v_high = v_values[j + 1].text
        v_low = v_values[j + 2].text
        v_close = v_values[j + 3].text
        v_volume = v_values[j + 6].text
        v_values_list.append([
            v_open, v_high, v_low, v_close, v_volume
        ])

    v_values_df = pd.DataFrame(v_values_list, columns=[
        'open', 'high', 'low', 'close', 'volume'])

    # 日付と株価を結合
    df = pd.concat(
        [pd.concat([v_date_df, v_values_df], axis=1), df], axis=0
    )

    return df


def fetch_stock_values(code):
    """
    株価のデータフレームを取得する
    """
    # 差分日数を計算する
    num_days = obtain_num_days(code)

    df = pd.DataFrame(
        [],
        columns=['date', 'open', 'high', 'low', 'close', 'volume']
    )

    if num_days >= 0:

        # ページネーションの数を計算する
        pagenation = min(math.ceil(num_days / 30), 10)

        for i in range(1, pagenation + 1):

            # 株価のページからデータフレームを取得する
            values_df = fetch_values_dataframe(code, i)
            df = pd.concat([df, values_df], axis=0)

    else:

        # 上場廃止された銘柄をリストから削除する
        delete_code(code)

    return df


def create_inserted_dataframe(df_new, code):
    """
    データベースに挿入するデータフレームを作る
    """
    query = f'select distinct * from \"{code}\" order by date desc limit 1;'
    conn = sqlite3.connect('stocks.db')
    with conn:
        df_old = pd.read_sql_query(query, conn)

    df_insert = pd.DataFrame(
        [],
        columns=['date', 'open', 'high', 'low', 'close', 'volume']
    )
    date_old_latest = datetime.strptime(df_old.iloc[0, 0], "%Y-%m-%d")

    if not df_new.empty:
        for i in range(0, len(df_new)):
            date_new_latest = datetime.strptime(df_new.iloc[i, 0], "%Y-%m-%d")
            if date_new_latest == date_old_latest:
                break
            df_insert = pd.concat([df_new.iloc[i, :].to_frame().T, df_insert])

    return df_insert


if __name__ == '__main__':

    # 銘柄コードのデータフレームを取得する
    codes_df = load_codes_dataframe()

    # プログレスバーを定義
    bar = tqdm(total=len(codes_df), dynamic_ncols=True,
               iterable=True, leave=False)
    bar.set_description('データを取得しています')

    for code in codes_df['code']:

        # 株価のデータフレームを取得する
        values_df = fetch_stock_values(code)
        values_df = create_inserted_dataframe(values_df, code)

        if not values_df.empty:

            # データベースに格納する
            conn = sqlite3.connect('stocks.db')
            with conn:
                values_df.to_sql(code, conn, if_exists='append', index=False)

        bar.update(1)
        time.sleep(2)
