import ccxt
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
binanceObj = ccxt.binance(config={
            'apiKey': api_key,
            'secret': secret,
            'enableRateLimit' : True,
            'options':{
                'defaultType':'future'
            }
        })

def timestamp_to_str(time):  # 타임스탬프 문자열로 변환
    return str(time.year * 100000000 + time.month * 1000000 + time.day * 10000 + time.hour * 100 + time.minute)


def day_to_mysql(table, start_time, day, ticker):
    for d in range(day):
        OHLCV = binanceObj.fetch_ohlcv(ticker, timeframe='1d', since=start_time + 86400000 * 100 * d, limit=100)#1번 반복이 한시간 +3600000 24시간 +86400000
        for i in range(len(OHLCV)):
            stamp = pd.to_datetime(OHLCV[i][0] * 1000000)
            times = timestamp_to_str(stamp)
            ranges = float((OHLCV[i][2] - OHLCV[i][3])).__round__(2)
            sql = '''INSERT INTO `{7}` (time, open, high, low, close, volume, ranges)
                VALUES({0}, {1}, {2}, {3}, {4}, {5}, {6})'''.format(times, OHLCV[i][1], OHLCV[i][2], OHLCV[i][3],
                                                                    OHLCV[i][4], OHLCV[i][5], ranges, table)
            cursor.execute(sql)
        db.commit()
        print(d)
        time.sleep(0.1)
def hour_to_mysql(table, start_time, day, ticker):
    for d in range(day):
        OHLCV = binanceObj.fetch_ohlcv(ticker, timeframe='1h', since=start_time + 3600000 * 500 * d, limit=500)#1번 반복이 한시간 +3600000 4시간 + 14400000,24시간 +86400000
        for i in range(len(OHLCV)):
            stamp = pd.to_datetime(OHLCV[i][0] * 1000000)
            times = timestamp_to_str(stamp)
            ranges = float((OHLCV[i][2] - OHLCV[i][3])).__round__(2)
            sql = '''INSERT INTO `{7}` (time, open, high, low, close, volume, ranges)
                VALUES({0}, {1}, {2}, {3}, {4}, {5}, {6})'''.format(times, OHLCV[i][1], OHLCV[i][2], OHLCV[i][3],
                                                                    OHLCV[i][4], OHLCV[i][5], ranges, table)
            cursor.execute(sql)
        db.commit()
        print(d)
        time.sleep(0.1)
def minute_to_mysql(table, start_time, day, ticker):
    for d in range(day):
        OHLCV = binanceObj.fetch_ohlcv(ticker, timeframe='15m', since=start_time + 900000 * 500 * d, limit=500)#1번 반복이 한시간 +3600000 24시간 +86400000
        for i in range(len(OHLCV)):
            stamp = pd.to_datetime(OHLCV[i][0] * 1000000)
            times = timestamp_to_str(stamp)
            ranges = float((OHLCV[i][2] - OHLCV[i][3])).__round__(2)
            sql = '''INSERT INTO `{7}` (time, open, high, low, close, volume, ranges)
                VALUES({0}, {1}, {2}, {3}, {4}, {5}, {6})'''.format(times, OHLCV[i][1], OHLCV[i][2], OHLCV[i][3],
                                                                    OHLCV[i][4], OHLCV[i][5], ranges, table)
            cursor.execute(sql)
        db.commit()
        print(d)
        time.sleep(0.1)
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
    for i in range(1,len(df)):
        sql = '''UPDATE {0} SET MA7 = {2}, MA25 = {3}, MA99 = {4}, upperBB = {5}, lowerBB={6} WHERE id ={1}'''\
            .format(table, df.loc[i]['id'], df.loc[i]['MA7'], df.loc[i]['MA25'], df.loc[i]['MA99'], df.loc[i]['upperBB'], df.loc[i]['lowerBB'])
        cursor.execute(sql)
    db.commit()

# load_last_time('btc_minute')
# law = load_last_time('btc_minute')[1]
# t = law[0:4]+'-'+law[4:6]+'-'+law[6:8]+' '+str(int(law[8:10]))+':'+law[10:12]+':'+law[12:14]+'00'#원형 :'2021-01-01 09:00:00'. 시간은 UTC기준이므로 +9시간
t = '2019-09-09 09:00:00'
start_time = int(time.mktime(datetime.strptime(t, '%Y-%m-%d %H:%M:%S').timetuple())*1000)#처음 데이터 가져올 때
# start_time = int(time.mktime(datetime.strptime(t, '%Y-%m-%d %H:%M:%S').timetuple())*1000)+60000 - 32400000#1분 차이 나면 +60000  9시간차이(UTC기준이므로)-32400000
# start_time = int(time.mktime(datetime.strptime(t, '%Y-%m-%d %H:%M:%S').timetuple())*1000)+900000 - 32400000#15분 차이나면 +900000 9시간차이-32400000
day = 10 #1000일
hour = 50# 1040일
minute = 200 #1040일
ticker = 'BNB/USDT'
day_to_mysql('bnb_day', start_time, day, ticker)
update_indicator('bnb_day')
hour_to_mysql('bnb_hour', start_time, hour, ticker)
update_indicator('bnb_hour')
minute_to_mysql('bnb_15minute', start_time, minute, ticker)
update_indicator('bnb_15minute')

