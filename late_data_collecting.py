import init
import pandas as pd
from datetime import datetime
import time
import pymysql

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

tf_table = [['1m', 'realtime_btc_minute'], ['15m', 'realtime_btc_15minute'],
             ['1h', 'realtime_btc_hour'], ['4h', 'realtime_btc_4hour'], ['1d', 'realtime_btc_day']]#1열은 timeframe, 2열은 테이블 명

def minute_to_mysql(table, timeframe):
    btc_ohlcv = init.binanceObj.fetch_ohlcv("BTC/USDT", timeframe=timeframe, limit=100)#1번 반복이 한시간 +3600000 24시간 +86400000
    for i in range(len(btc_ohlcv)):
        stamp = pd.to_datetime(btc_ohlcv[i][0] * 1000000)
        times = init.timestamp_to_str(stamp)#MySQL에는 UTC시간으로 저장
        sql = '''INSERT INTO `{6}` (time, open, high, low, close, volume) 
            VALUES({0}, {1}, {2}, {3}, {4}, {5})'''.format(times, btc_ohlcv[i][1], btc_ohlcv[i][2], btc_ohlcv[i][3],
                                                           btc_ohlcv[i][4], btc_ohlcv[i][5], table)
        cursor.execute(sql)
    db.commit()
    time.sleep(0.1)

now = datetime.now()
t = now.strftime('%Y-%m-%d %H:%M:%S')
start_time = int(time.mktime(datetime.strptime(t, '%Y-%m-%d %H:%M:%S').timetuple())*1000)-32400000#우리나라가 9시간 빠르기 때문에 -9시간 한다.


def update_indicator(table):#OHLCV기반으로 지표 생성 후 DB 테이블 업데이트
    sql = '''SELECT * FROM {0}'''.format(table)
    cursor.execute(sql)
    result = cursor.fetchall()
    df = pd.DataFrame(result)
    df['MA7'] = df['close'].rolling(window=7, min_periods=1).mean()
    df['MA25'] = df['close'].rolling(window=25, min_periods=1).mean()
    df['MA99'] = df['close'].rolling(window=99, min_periods=1).mean()
    df['stddev'] = df['close'].rolling(window=25, min_periods=1).std()
    df['upperBB'] = df['MA25'] + df['stddev'] * 2
    df['lowerBB'] = df['MA25'] - df['stddev'] * 2
    for i in range(len(df)-1,len(df)):#마지막 행만 지표가 있으면 된다.
        sql = '''UPDATE `{0}` SET MA7 = {2}, MA25 = {3}, MA99 = {4}, upperBB = {5}, lowerBB={6} WHERE id ={1}'''.format(table, df.loc[i]['id'], df.loc[i]['MA7'], df.loc[i]['MA25'], df.loc[i]['MA99'], df.loc[i]['upperBB'], df.loc[i]['lowerBB'])
        # sql = '''UPDATE `{0}` SET MA7 = {2}, MA25 = {3}, MA99 = {4}, upperBB = {5}, lowerBB={6} WHERE id ={1}'''.format(table, df.loc[i]['id'], df.loc[i]['MA7'], df.loc[i]['MA25'], df.loc[i]['MA99'])
        cursor.execute(sql)
    db.commit()
    print(table+": 완료")

for tf in tf_table:#각 분봉, 시간봉, 일봉 당 최근 100개의 데이터 mysql에 저장
    minute_to_mysql(tf[1],tf[0])
for tf in tf_table:#최근 시간 지표 셋팅
    update_indicator(tf[1])




