import ccxt
import pandas as pd
from datetime import datetime
import time
import pymysql
import schedule
# import Stretegy

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

class Binance:
    order_list = []#클래스 변수
    stand_by_orders = []
    def __init__(self):
        # 선물거래용 바이낸스 객체 생성
        self.binanceObj = ccxt.binance(config={
            'apiKey': api_key,
            'secret': secret,
            'enableRateLimit' : True,
            'options':{
                'defaultType':'future'
            }
        })
        self.tf_table = [['1m', 'realtime_btc_minute', 1], ['5m', 'realtime_btc_5minute', 5], ['15m', 'realtime_btc_15minute', 15],
                    ['1h', 'realtime_btc_hour', 60], ['4h', 'realtime_btc_4hour', 240], ['1d', 'realtime_btc_day', 3600]]  # 1열은 timeframe, 2열은 테이블 명
    def timestamp_to_str(self, time): # 타임스탬프 문자열로 변환
        return str(time.year*100000000 + time.month*1000000 + time.day*10000 + time.hour*100 + time.minute)
    def clear_realtime_table(self):
        for tf in self.tf_table:#realtime 전체 테이블 삭제
            sql = '''TRUNCATE `{0}`'''.format(tf[1])
            cursor.execute(sql)
            db.commit()
        print("전체 realtime_table 초기화 완료")
        print("================================================================================")
    def minute_to_mysql(self, table, timeframe):
        btc_ohlcv = self.binanceObj.fetch_ohlcv("BTC/USDT", timeframe=timeframe, limit=100)  # 1번 반복이 한시간 +3600000 24시간 +86400000
        for i in range(len(btc_ohlcv)-1):
            stamp = pd.to_datetime(btc_ohlcv[i][0] * 1000000)
            times = self.timestamp_to_str(stamp)  # MySQL에는 UTC시간으로 저장
            Range = (btc_ohlcv[i][2] - btc_ohlcv[i][3]).__round__(2)
            sql = '''INSERT INTO `{0}` (time, open, high, low, close, volume, ranges) VALUES({1}, {2}, {3}, {4}, {5}, {6}, {7})'''.\
                format(table, times, btc_ohlcv[i][1], btc_ohlcv[i][2], btc_ohlcv[i][3],
                                                               btc_ohlcv[i][4], btc_ohlcv[i][5], Range)
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
            # sql = '''UPDATE `{0}` SET MA7 = {2}, MA25 = {3}, MA99 = {4}, upperBB = {5}, lowerBB={6} WHERE id ={1}'''.format(table, df.loc[i]['id'], df.loc[i]['MA7'], df.loc[i]['MA25'], df.loc[i]['MA99'])
            cursor.execute(sql)
        db.commit()
        print(table + ": 완료")

    def load_last_time(self, table):#mysql에 저장되있는 마지막 시간대 불러오기
        sql = '''SELECT * FROM `{0}` ORDER BY id DESC LIMIT 1'''.format(table)
        cursor.execute(sql)
        result = cursor.fetchall()
        return result[0]['id'], result[0]['time']

    def update_indicator(self, table, diff):  # OHLCV기반으로 지표 생성 후 DB 테이블 업데이트
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
            # sql = '''UPDATE `{0}` SET MA7 = {2}, MA25 = {3}, MA99 = {4}, upperBB = {5}, lowerBB={6} WHERE id ={1}'''.format(table, df.loc[i]['id'], df.loc[i]['MA7'], df.loc[i]['MA25'], df.loc[i]['MA99'])
            cursor.execute(sql)
        db.commit()
        print(table + ": 지표 업데이트 완료")

    def enter_position(self, enter_price, exit_price, stop_market, algo, pos_direction):#algo:전략 유형, pos_dirction:롱/숏 방향 0=롱 1=숏
        symbol = 'BTC/USDT'
        balance = self.binanceObj.fetch_balance()
        availableBalance = balance['info']['assets'][1]['availableBalance']
        amount = 0.001
        #지정가 매수, 스탑로스 주문
        if pos_direction ==0:
            enter_order = self.binanceObj.create_order(symbol, 'limit', 'buy', amount, enter_price)
            stop_order = self.binanceObj.create_order(symbol, 'STOP_MARKET', 'sell', amount,
                                                      params={'stopPrice': stop_market})
        else:
            enter_order = self.binanceObj.create_order(symbol, 'limit', 'sell', amount, enter_price)
            stop_order = self.binanceObj.create_order(symbol, 'STOP_MARKET', 'buy', amount,
                                                      params={'stopPrice': stop_market})

        print("주문 완료")
        enter_order['algo'] = algo
        enter_order['algo_side'] = 'enter'
        stop_order['algo'] = algo
        stop_order['algo_side'] = 'stop_market'
        self.stand_by_orders.append({'algo':algo, 'price':exit_price, 'amount':amount})
        self.order_list.append(stop_order) # stop_market먼저 order_list에 추가
        self.order_list.append(enter_order)


    def check_orders(self):
        print("===============================CHECK ORDERS=====================================")
        for order in self.order_list:
            status = self.binanceObj.fetch_order_status(order['info']['orderId'], 'BTC/USDT')
            side = order['algo_side']
            algo = order['algo']#같은 전략
            print(algo, status, side)
            if status == 'closed' and side == 'stop_market':#시나리오1(드문 상황) 지정가 매수 후 급락 ->stop_market 까지 체결. 손절로 마무리.
                for o in self.order_list:
                    if algo == o['algo']: #order_list에서 stop_market제거 후 다음 인덱스인 enter도 제거
                        index = self.order_list.index(o)
                        self.order_list.remove(o)
                        self.order_list.pop(index)
            if status == 'closed' and side == 'enter':#시나리오2(일반적인 상황) 지정가 매수 까지 체결 -> 지정가 매수 제거 후 지정가 매도 주문
                for o in self.order_list:
                    stat = self.binanceObj.fetch_order_status(o['info']['orderId'], 'BTC/USDT')
                    if algo == o['algo'] and stat == 'closed':
                        self.order_list.remove(o)
                for s in self.stand_by_orders:
                    if algo == s['algo']:
                        exit_order = self.binanceObj.create_order('BTC/USDT', 'limit', 'sell', s['amount'], s['price'])
                        exit_order['algo'] = algo
                        exit_order['algo_side'] = 'exit'
                        print("포지션 진입 후 익절 주문 완료")
                        self.order_list.append(exit_order)
                        self.stand_by_orders.remove(s)
            if status == 'closed' and side =='exit':#지정가 매수 후 지정가 매도까지 체결 -> 지정가 주문
                for o in self.order_list:
                    if algo == o['algo'] and o['algo_side'] == 'stop_market':#stop_market이 미체결인 경우
                        self.binanceObj.cancel_order(o['info']['orderId'], 'BTC/USDT')#주문 취소
                        self.order_list.remove(o)#stop_market 리스트에서 제거
                self.order_list.remove(order)#지정가 매도주문도 리스트에서 제거


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
                btc_ohlcv = self.binanceObj.fetch_ohlcv("BTC/USDT", timeframe=tf[0], limit=2)
                #여기서 for문
                stamp = pd.to_datetime(btc_ohlcv[0][0] * 1000000)
                times = self.timestamp_to_str(stamp)  # MySQL에는 UTC시간으로 저장
                Range = (btc_ohlcv[0][2] - btc_ohlcv[0][3]).__round__(2)
                sql = '''INSERT INTO `{0}` (time, open, high, low, close, volume, ranges) 
                    VALUES({1}, {2}, {3}, {4}, {5}, {6}, {7})'''.format(tf[1], times, btc_ohlcv[0][1], btc_ohlcv[0][2],
                                                                   btc_ohlcv[0][3],
                                                                   btc_ohlcv[0][4], btc_ohlcv[0][5], Range)
                cursor.execute(sql)
                db.commit()
                self.update_indicator(tf[1], diff)

        print("현재시각: "+str(datetime.now()))
        print("================================================================================")

    def are_algos_working(self):
        stObj = Stretegy()
        stand_by_algo_set = set()
        all_algo_set = set(['algo2_pre_low', 'algo3_pre_high'])
        for order in self.order_list:
            stand_by_algo_set.add(order['algo'])
        not_working_algo = all_algo_set - stand_by_algo_set
        print(stand_by_algo_set)
        print(not_working_algo)
        for i in not_working_algo:
            if i == 'algo2_pre_low':
                print("algo#2 재 진입")
                stObj.algo2_pre_low('realtime_btc_minute')
            if i == 'algo3_pre_high':
                print("algo#3 재 진입")
                stObj.algo3_pre_high('realtime_btc_minute')


    def main(self):
        self.clear_realtime_table()
        for tf in self.tf_table:  # 각 분봉, 시간봉, 일봉 당 최근 100개의 데이터 mysql에 저장
            self.minute_to_mysql(tf[1], tf[0])
        for tf in self.tf_table:  # 최근 시간 지표 셋팅
            self.setting_indicator(tf[1])
        print("================================================================================")
        schedule.every().minutes.at(":10").do(self.is_it_late_data, tf_table = self.tf_table)
        schedule.every(10).seconds.do(self.check_orders)
        schedule.every(1).minutes.do(self.are_algos_working)

class Stretegy(Binance):
    def __init__(self):
        super().__init__()
    def screen_low(self, table, id, count, range):
        sql = '''SELECT *FROM `{0}` WHERE low = (SELECT MIN(low)FROM `{0}` WHERE id>{1}-{2} and id<={1});'''\
            .format(table, id, range)
        cursor.execute(sql)
        result = cursor.fetchall()
        if id == result[0]['id']:  # 최 우측이 low일 때
            if count == 0:  # 첫 번째 스크리닝 이면 range를 2배로 늘림
                return self.screen_low(table, id - 1, count + 1, range*2)
            else:  # low 채택
                return result[0]['low']
        else:
            print(id)
            return self.screen_low(table, result[0]['id'], count + 1, 4)
    def screen_high(self, table, id, count, range):
        sql = '''SELECT *FROM `{0}` WHERE high = (SELECT MAX(high)FROM `{0}` WHERE id>{1}-{2} and id<={1});''' \
            .format(table, id, range)
        cursor.execute(sql)
        result = cursor.fetchall()
        if id == result[0]['id']:  # 최 우측이 low이면
            if count == 0:  # 첫 번째 스크리닝시 직전 캔들이 저점이면 직전 저점보다 더 낮은 캔들을 찾는다.
                return self.screen_high(table, id - 1, count + 1, range*2)
            else:  # low 채택
                return result[0]['high']
        else:
            return self.screen_high(table, result[0]['id'], count + 1, 4)

    def algo2_pre_low(self, table):  # 직전 저점 제외, 최소 3캔들 이전
        sql = '''SELECT * FROM `{0}` ORDER BY id DESC LIMIT 1'''.format(table)
        cursor.execute(sql)
        result = cursor.fetchall()
        enter_price = self.screen_low(table, result[0]['id'], 0, 4)
        exit_price = float(enter_price) * 1.005  # 0.5% 단타
        stop_market = float(enter_price) * 0.995  # 0.5% 손절
        cur_price = self.binanceObj.fetch_ticker("BTC/USDT")
        print("현재가: " + str(cur_price['close']) +" 지정가 매수: " + str(enter_price) + ",  익절가: " + str(exit_price) + ",  손절가: " + str(stop_market))
        self.enter_position(enter_price, exit_price, stop_market, 'algo2_pre_low', 0)

    def algo3_pre_high(self, table):  # 직전 저점 제외, 최소 3캔들 이전
        sql = '''SELECT * FROM `{0}` ORDER BY id DESC LIMIT 1'''.format(table)
        cursor.execute(sql)
        result = cursor.fetchall()
        enter_price = self.screen_high(table, result[0]['id'], 0, 4)
        exit_price = float(enter_price) * 0.995  # 0.5% 단타
        stop_market = float(enter_price) * 1.005  # 0.5% 손절
        cur_price = self.binanceObj.fetch_ticker("BTC/USDT")
        print("현재가: " + str(cur_price['close']) + " 진입 가격: " + str(enter_price) + ",  수익 실현 가격: " + str(
            exit_price) + ",  손절 가격: " + str(stop_market))
        self.enter_position(enter_price, exit_price, stop_market, 'algo3_pre_high', 1)



if __name__ == "__main__":
    BObj = Binance()
    BObj.main()
    while True:
        # schedule.every(10).seconds.do(is_it_late_data(tf_table))
        schedule.run_pending()