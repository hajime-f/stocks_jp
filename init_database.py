"""
データベースを初期化するためのスクリプト
"""
import sqlite3
import time
import urllib.request

import pandas as pd
from bs4 import BeautifulSoup
from tqdm import tqdm


def fetch_stock_values(code):
    """
    株価のデータフレームを取得する
    """
    df = pd.DataFrame([], columns=['open', 'high', 'low', 'close', 'volume'])

    for i in range(1, 11):

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

    df = df.sort_values('date')
    df = df.reset_index(drop=True)

    return df


def is_table_exists(code):

    conn = sqlite3.connect('stocks.db')
    curs = conn.cursor()

    # テーブル存在確認
    curs.execute(
        f'select count(*) from sqlite_master where type="table" and name=\"{code}\";'
    )
    if curs.fetchone() == (0,):
        return False
    else:
        return True


if __name__ == '__main__':

    # CSV を読み込む
    codes_df = pd.read_csv('codes.csv')

    # データベースに保存
    conn = sqlite3.connect('stocks.db')
    with conn:
        codes_df.to_sql('Symbols', conn, if_exists='replace', index=False)

    # プログレスバーを定義
    bar = tqdm(total=len(codes_df), dynamic_ncols=True,
               iterable=True, leave=False)
    bar.set_description('データを取得しています')

    for code in codes_df['code']:

        if not is_table_exists(code):

            # 株価のデータフレームを取得
            values_df = fetch_stock_values(code)

            # データベースに保存
            conn = sqlite3.connect('stocks.db')
            with conn:
                values_df.to_sql(code, conn, if_exists='replace', index=False)

            time.sleep(1)

        bar.update(1)
