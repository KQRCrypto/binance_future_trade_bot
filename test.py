import main
import pandas as pd
from datetime import datetime
import time
import ccxt
import pymysql
import matplotlib.pyplot as plt
import schedule
import pprint
with open("../api.txt")as f:
    lines = f.readlines()
    api_key = lines[0].strip()
    secret = lines[1].strip()
    db = lines[2].strip()
    password = lines[3].strip()
db = pymysql.connect(
        user = 'root',
        passwd = password,
        host = '127.0.0.1',
        db = db,
        charset= 'utf8'
)
cursor = db.cursor(pymysql.cursors.DictCursor)
binanceObj = ccxt.binance(config={
            'apiKey': api_key,
            'secret': secret,
            'enableRateLimit' : True,
            'options':{
                'defaultType':'future'
            }
        })
tf_table = [['1m', 'realtime_btc_minute', 1], ['15m', 'realtime_btc_15minute', 15],
             ['1h', 'realtime_btc_hour', 60], ['4h', 'realtime_btc_4hour', 240], ['1d', 'realtime_btc_day', 3600]]#1열은 timeframe, 2열은 테이블 명


def target_price(table):  # mysql에 저장되있는 마지막 시간대 불러오기
    sql = '''SELECT * FROM `{0}` ORDER BY id DESC LIMIT 2'''.format(table)
    cursor.execute(sql)
    result = cursor.fetchall()
    # pprint.pprint(result)
    range = (result[1]['high'] - result[1]['low'])
    long_target = result[0]['open'] + range
    short_target = result[0]['open'] - range
    return long_target, short_target
id = 'binance'


def enter_position():
    binanceObj.create_limit_buy_order('BTC/USDT', 0.001, 35000)
binanceObj.create_limit_buy_order('BTC/USDT', 0.001, 35000)
binanceObj.fetch_balance()
target_price(tf_table[1][1])
btc = binanceObj.fetch_ticker("BTC/USDT")
pprint.pprint(btc)
print(btc['last'])



#지표 가져오기
result_list = []
def bring_indicator(tf_table):
    btc = binanceObj.fetch_ticker("BTC/USDT")
    cur_price = btc['last']
    for tf in tf_table:
        sql = '''SELECT * FROM `{0}` ORDER BY id DESC LIMIT 99'''.format(tf[1])
        cursor.execute(sql)
        result = cursor.fetchall()
        result = pd.DataFrame(result)
        result['bandwidth'] = (result.upperBB - result.lowerBB)/cur_price*100
        result['percentBand'] = (cur_price - result.lowerBB)/(result.upperBB - result.lowerBB)
        result_list.append(result)
bring_indicator(tf_table)
# plot


def visualization():
    for i in range(4):
        plt.subplot(411+i)
        df = result_list[i]
        plt.plot(df.id, df['close'], color='green', label='close')
        plt.plot(df.id, df['MA25'], 'k--', label='MA25')
        plt.plot(df.id, df.upperBB, 'r--', label='upper')
        plt.plot(df.id, df.lowerBB, 'b--', label='lower')
        plt.fill_between(df.id, df.upperBB, df.lowerBB, color='0.95')
        for i in range(len(df.id)):
            if df.close[i]*0.995<=df.lowerBB[i]:
            # if df.percentBand[i] > 0 and df.percentBand[i] < 0.2:
                    plt.plot(df.id[i], df.close[i], 'r^')
            elif df.close[i]*1.005>=df.upperBB[i]:
            # elif df.percentBand[i] > 0.8 and df.percentBand[i] < 1:
                plt.plot(df.id[i], df.close[i], 'bv')
        plt.grid(True)
        plt.ylim([min(df.lowerBB), max(df.upperBB)])
        plt.legend(loc='best')
# plt.figure(figsize=(10, 10))
# visualization()
# plt.show()

#현재가




if __name__ == "__main__":

    while True:
        # schedule.every(10).seconds.do(is_it_late_data(tf_table))
        schedule.run_pending()
