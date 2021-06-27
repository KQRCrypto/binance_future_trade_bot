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

tf_table = [['1m', 'realtime_btc_minute', 1], ['15m', 'realtime_btc_15minute', 15],
             ['1h', 'realtime_btc_hour', 60], ['4h', 'realtime_btc_4hour', 240], ['1d', 'realtime_btc_day', 3600]]#1열은 timeframe, 2열은 테이블 명

def load_last_time(table):#mysql에 저장되있는 마지막 시간대 불러오기
    sql = '''SELECT * FROM `{0}` ORDER BY id DESC LIMIT 1'''.format(table)
    cursor.execute(sql)
    result = cursor.fetchall()
    return result[0]['id'], result[0]['time']

def is_it_late_data(tf_table):
    for tf in tf_table:
        id = load_last_time(tf[1])[0]
        law = load_last_time(tf[1])[1]
        t = law[0:4] + '-' + law[4:6] + '-' + law[6:8] + ' ' + str(int(law[8:10])) + ':' + law[10:12] + ':' + law[12:14] + '00'  # 원형 :'2021-01-01 09:00:00'. 시간은 UTC기준이므로 +9시간
        late_time = int(time.mktime(datetime.strptime(t, '%Y-%m-%d %H:%M:%S').timetuple()) * 1000)  # 처음 데이터 가져올 때
        cur_time = init.binanceObj.fetch_ohlcv("BTC/USDT", timeframe=tf[0], limit=1)[0][0] - 32400000
        print(cur_time-late_time, tf[2])
        if cur_time-60000*tf[2] < late_time:
            print("최신 데이터이므로 업데이트하지 않습니다.")
            break
        else:
            print("최신 데이터를 추가 저장 합니다.")
            diff = int((cur_time - late_time)/60000)+1# 가장 최근에 저장한 데이터는 봉 마감전 데이터므로 다시 업데이트 한다.
            btc_ohlcv = init.binanceObj.fetch_ohlcv("BTC/USDT", timeframe=tf[0], limit=diff)
            for i in range(len(btc_ohlcv)):
                stamp = pd.to_datetime(btc_ohlcv[i][0] * 1000000)
                times = init.timestamp_to_str(stamp)  # MySQL에는 UTC시간으로 저장
                if i == 0 :#가장 최근에 저장한 데이터는 봉 마감전 데이터므로 다시 업데이트 한다.
                    sql = '''UPDATE `{0}` SET open={1}, high={2}, low={3}, close={4}, volume={5} WHERE id={6}'''.format(tf[1], btc_ohlcv[i][1], btc_ohlcv[i][2], btc_ohlcv[i][3],
                                                                   btc_ohlcv[i][4], btc_ohlcv[i][5], id)
                    cursor.execute(sql)
                else:
                    sql = '''INSERT INTO `{6}` (time, open, high, low, close, volume) 
                        VALUES({0}, {1}, {2}, {3}, {4}, {5})'''.format(times, btc_ohlcv[i][1], btc_ohlcv[i][2], btc_ohlcv[i][3],
                                                                       btc_ohlcv[i][4], btc_ohlcv[i][5], tf[1])
                    cursor.execute(sql)
            db.commit()
            print("최신화 완료")
is_it_late_data(tf_table)


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




