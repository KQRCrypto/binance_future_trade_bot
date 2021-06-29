import ccxt
import pprint
import main
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

def minute_to_mysql(table, start_time, timeframe_limit, day):#iter : 반복일자 수
    for d in range(day):
        btc_ohlcv = main.binanceObj.fetch_ohlcv("BTC/USDT", timeframe=timeframe_limit[0], since=start_time + timeframe_limit[1] * d, limit=timeframe_limit[2])#1번 반복이 한시간 +3600000 24시간 +86400000
        # btc_ohlcv = binanc4e.fetch_ohlcv("BTC/USDT", timeframe='15m', since=start_time+86400000*i, limit=4*24)#1번 반복이 한시간 +3600000 24시간 +86400000
        for i in range(len(btc_ohlcv)):
            stamp = pd.to_datetime(btc_ohlcv[i][0] * 1000000)
            times = main.timestamp_to_str(stamp)
            sql = '''INSERT INTO `{6}` (time, open, high, low, close, volume)
                VALUES({0}, {1}, {2}, {3}, {4}, {5})'''.format(times, btc_ohlcv[i][1], btc_ohlcv[i][2], btc_ohlcv[i][3],
                                                               btc_ohlcv[i][4], btc_ohlcv[i][5], table)
            cursor.execute(sql)
        db.commit()
        print(d)
        time.sleep(0.1)

def load_last_time(table):#mysql에 저장되있는 마지막 시간대 불러오기
    sql = '''SELECT * FROM `{0}` ORDER BY id DESC LIMIT 1'''.format(table)
    cursor.execute(sql)
    result = cursor.fetchall()
    return result[0]['id'], result[0]['time']

load_last_time('btc_minute')
law = load_last_time('btc_minute')[1]
t = law[0:4]+'-'+law[4:6]+'-'+law[6:8]+' '+str(int(law[8:10]))+':'+law[10:12]+':'+law[12:14]+'00'#원형 :'2021-01-01 09:00:00'. 시간은 UTC기준이므로 +9시간
t = '2021-01-01 09:00:00'
start_time = int(time.mktime(datetime.strptime(t, '%Y-%m-%d %H:%M:%S').timetuple())*1000)#처음 데이터 가져올 때
start_time = int(time.mktime(datetime.strptime(t, '%Y-%m-%d %H:%M:%S').timetuple())*1000)+60000 - 32400000#1분 차이 나면 +60000  9시간차이(UTC기준이므로)-32400000
start_time = int(time.mktime(datetime.strptime(t, '%Y-%m-%d %H:%M:%S').timetuple())*1000)+900000 - 32400000#15분 차이나면 +900000 9시간차이-32400000
day = 1
#1분, 한시간단위로 req이므로 1분 timestamp(60000)*60, ,하루 반복이면 *24
timeframe_limit = [['1m', 3600000, 60, 24], ['15m', 3600000*24, 4*24, 1]]#1열은 timeframe, 2열은 한 번 반복시 한시간이면 3600000 3열은 api에서 한 번에 가져오는 행 수
minute_to_mysql('btc_minute',start_time, timeframe_limit[0],day*timeframe_limit[0][3])

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
    for i in range(len(df)):
        sql = '''UPDATE {0} SET MA7 = {2}, MA25 = {3}, MA99 = {4}, upperBB = {5}, lowerBB={6}, WHERE id ={1}'''\
            .format(table, df.loc[i]['id'], df.loc[i]['MA7'], df.loc[i]['MA25'], df.loc[i]['MA99'],  df.loc[i]['upperBB'],  df.loc[i]['lowerBB'])
        cursor.execute(sql)
    db.commit()

update_indicator('btc_minute')



#private api
#잔고조회
balance = main.binanceObj.fetch_balance(params={'type': "future"})
print(balance['USDT'])