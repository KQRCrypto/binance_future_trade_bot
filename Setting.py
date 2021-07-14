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
    user='root',
    passwd=password,
    host='127.0.0.1',
    db=db,
    charset='utf8'
)
cursor = db.cursor(pymysql.cursors.DictCursor)

class Setting:
    # tf_all_table : 가능한 모든 테이블
    # tf_table : 속도 향상을 위해 사용할 테이블
    available_table = [['1m', 'realtime_btc_minute', 1, 'BTC/USDT'], ['5m', 'realtime_btc_5minute', 5, 'BTC/USDT'],
                            ['15m', 'realtime_btc_15minute', 15, 'BTC/USDT'],
                            ['15m', 'realtime_eth_15minute', 15, 'ETH/USDT'],
                            ['1h', 'realtime_btc_hour', 60, 'BTC/USDT'], ['1h', 'realtime_eth_hour', 60, 'ETH/USDT'],
                            ['1h', 'realtime_ada_hour', 60, 'ADA/USDT'],
                            ['4h', 'realtime_btc_4hour', 240, 'BTC/USDT'],
                            ['4h', 'realtime_eth_4hour', 240, 'ETH/USDT'], ['4h', 'realtime_eth_hour', 240, 'ADA/USDT'],
                            ['1d', 'realtime_btc_day', 3600, 'BTC/USDT'], ['1d', 'realtime_eth_day', 3600, 'ETH/USDT'],
                            ['1d', 'realtime_ada_hour', 3600, 'ADA/USDT']]  # 1열은 timeframe, 2열은 테이블 명

    execute_table = [['1m', 'realtime_btc_minute', 1, 'BTC/USDT'],
                          ['15m', 'realtime_eth_15minute', 15, 'ETH/USDT'], ['1h', 'realtime_ada_hour', 60, 'ADA/USDT']]

    def timestamp_to_str(self, time):  # 타임스탬프 문자열로 변환
        return str(time.year * 100000000 + time.month * 1000000 + time.day * 10000 + time.hour * 100 + time.minute)

    def clear_realtime_table(self):
        for tf in self.execute_table:  # realtime 전체 테이블 삭제
            sql = '''TRUNCATE `{0}`'''.format(tf[1])
            cursor.execute(sql)
            db.commit()
        print("전체 realtime_table 초기화 완료")
        print("================================================================================")

    def ohlcv_to_mysql(self, table, timeframe, ticker):
        OHLCV = self.binanceObj.fetch_ohlcv(ticker, timeframe=timeframe,
                                            limit=100)  # 1번 반복이 한시간 +3600000 24시간 +86400000
        for i in range(len(OHLCV) - 1):
            stamp = pd.to_datetime(OHLCV[i][0] * 1000000)
            times = self.timestamp_to_str(stamp)  # MySQL에는 UTC시간으로 저장
            Range = (OHLCV[i][2] - OHLCV[i][3]).__round__(2)
            sql = '''INSERT INTO `{0}` (time, open, high, low, close, volume, ranges) VALUES({1}, {2}, {3}, {4}, {5}, {6}, {7})'''. \
                format(table, times, OHLCV[i][1], OHLCV[i][2], OHLCV[i][3],
                       OHLCV[i][4], OHLCV[i][5], Range)
            cursor.execute(sql)
        db.commit()
        time.sleep(0.1)

    def setting_indicator(self, table):  # OHLCV기반으로 지표 생성 후 DB 테이블 업데이트
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
        for i in range(1, len(df)):  # 마지막 행만 지표가 있으면 된다.
            sql = '''UPDATE `{0}` SET MA7 = {2}, MA25 = {3}, MA99 = {4}, upperBB = {5}, lowerBB={6} WHERE id ={1}'''.format(
                table, df.loc[i]['id'], df.loc[i]['MA7'], df.loc[i]['MA25'], df.loc[i]['MA99'], df.loc[i]['upperBB'],
                df.loc[i]['lowerBB'])
            cursor.execute(sql)
        db.commit()
        print(table + ": 완료")

    def load_last_time(self, table):  # mysql에 저장되있는 마지막 시간대 불러오기
        sql = '''SELECT * FROM `{0}` ORDER BY id DESC LIMIT 1'''.format(table)
        cursor.execute(sql)
        result = cursor.fetchall()
        return result[0]['id'], result[0]['time']

    def update_indicator(self, table):  # OHLCV기반으로 지표 생성 후 DB 테이블 업데이트
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
        diff = 1
        for i in range(len(df) - diff, len(df)):  # 마지막 행만 지표가 있으면 된다.
            sql = '''UPDATE `{0}` SET MA7 = {2}, MA25 = {3}, MA99 = {4}, upperBB = {5}, lowerBB={6} WHERE id ={1}'''.format(
                table, df.loc[i]['id'], df.loc[i]['MA7'], df.loc[i]['MA25'], df.loc[i]['MA99'], df.loc[i]['upperBB'],
                df.loc[i]['lowerBB'])
            cursor.execute(sql)
        db.commit()
        print(table + ": 지표 업데이트 완료")

    def is_it_late_data(self, tf_table):
        for tf in tf_table:
            law = self.load_last_time(tf[1])[1]
            t = law[0:4] + '-' + law[4:6] + '-' + law[6:8] + ' ' + str(int(law[8:10])) + ':' + law[10:12] + ':' + law[12:14] + '00'  # 원형 :'2021-01-01 09:00:00'. 시간은 UTC기준이므로 +9시간
            late_time = int(time.mktime(datetime.strptime(t, '%Y-%m-%d %H:%M:%S').timetuple()) * 1000)  # 처음 데이터 가져올 때
            cur_time = self.binanceObj.fetch_ohlcv("BTC/USDT", timeframe=tf[0], limit=1)[0][0] - 32400000
            # print("최신 업데이트 후 "+str((cur_time - late_time)/1000)+"초 경과 했습니다."+str(tf[2])+" 봉 업데이트 심사")
            #왜 6만차이야? 12만 차이 나야하는거 아냐?
            # print(cur_time - late_time)
            if cur_time - late_time == 60000:
                print("바이낸스 딜레이로 인한 문제 발생")
            if cur_time - 60000 * tf[2] <= late_time:
                print(tf[1]+"는 최신 데이터이므로 업데이트하지 않습니다.")
                break
            else:
                print(tf[1]+"에 최신 데이터를 추가 저장 합니다.")
                diff = int((cur_time - late_time) / 60000)  # 가장 최근에 저장한 데이터는 봉 마감전 데이터므로 다시 업데이트 한다.
                OHLCV = self.binanceObj.fetch_ohlcv(tf[3], timeframe=tf[0], limit=2)
                #여기서 for문
                stamp = pd.to_datetime(OHLCV[0][0] * 1000000)
                times = self.timestamp_to_str(stamp)  # MySQL에는 UTC시간으로 저장
                Range = (OHLCV[0][2] - OHLCV[0][3]).__round__(2)
                sql = '''INSERT INTO `{0}` (time, open, high, low, close, volume, ranges) 
                    VALUES({1}, {2}, {3}, {4}, {5}, {6}, {7})'''.format(tf[1], times, OHLCV[0][1], OHLCV[0][2],
                                                                        OHLCV[0][3],
                                                                        OHLCV[0][4], OHLCV[0][5], Range)
                cursor.execute(sql)
                db.commit()
                self.update_indicator(tf[1])

        print("현재시각: "+str(datetime.now()))
        print("================================================================================")