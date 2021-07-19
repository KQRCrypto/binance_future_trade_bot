import pandas as pd
from datetime import datetime
import time
import ccxt
import pymysql
import schedule
from Setting import Setting

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


class Binance(Setting):
    order_list = []#클래스 변수
    stand_by_orders = []
    timeout_orders = [] # 캔들 생성 시 마다 초기화 해주는 전략 들
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

    def update_indicator(self, table, ticker):  # OHLCV기반으로 지표 생성 후 DB 테이블 업데이트
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
        stretegyObj = Stretegy()
        stretegyObj.algo4_bollinger_band(table, ticker)

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
                stamp = pd.to_datetime(OHLCV[0][0] * 1000000)
                times = self.timestamp_to_str(stamp)  # MySQL에는 UTC시간으로 저장
                Range = (OHLCV[0][2] - OHLCV[0][3]).__round__(2)
                sql = '''INSERT INTO `{0}` (time, open, high, low, close, volume, ranges) 
                    VALUES({1}, {2}, {3}, {4}, {5}, {6}, {7})'''.format(tf[1], times, OHLCV[0][1], OHLCV[0][2],
                                                                        OHLCV[0][3],
                                                                        OHLCV[0][4], OHLCV[0][5], Range)
                cursor.execute(sql)
                db.commit()
                self.update_indicator(tf[1], tf[3])

        print("현재시각: "+str(datetime.now()))
        print("================================================================================")

    def enter_position(self, enter_price, exit_price, stop_market, algo, pos_direction, ticker):#algo:전략 유형, pos_dirction:롱/숏 방향 0=롱 1=숏
        balance = self.binanceObj.fetch_balance()
        availableBalance = balance['info']['assets'][1]['availableBalance']
        cur_price = self.binanceObj.fetch_ticker(ticker)
        amount = 0.001
        if ticker=='ETH/USDT':amount=0.01
        elif ticker =='ADA/USDT':amount=50
        #지정가 매수, 스탑로스 주문
        if pos_direction ==0:
            enter_order = self.binanceObj.create_order(ticker, 'limit', 'buy', amount, enter_price)
            stop_order = self.binanceObj.create_order(ticker, 'STOP_MARKET', 'sell', amount,
                                                      params={'stopPrice': stop_market})
        else:
            enter_order = self.binanceObj.create_order(ticker, 'limit', 'sell', amount, enter_price)
            stop_order = self.binanceObj.create_order(ticker, 'STOP_MARKET', 'buy', amount,
                                                      params={'stopPrice': stop_market})
        enter_order['algo'] = algo
        enter_order['algo_side'] = 'enter'
        enter_order['ticker'] = ticker
        stop_order['algo'] = algo
        stop_order['algo_side'] = 'stop_market'
        stop_order['ticker'] = ticker
        self.stand_by_orders.append({'algo':algo, 'price':exit_price, 'amount':amount, 'direction':pos_direction, 'ticker':ticker})
        self.order_list.append(stop_order) # stop_market먼저 order_list에 추가
        self.order_list.append(enter_order)

    def timeout_position(self, enter_price, exit_price, stop_market,algo, pos_direction, ticker):
        amount = 0.005
        if ticker == 'ETH/USDT':
            amount = 0.1
        elif ticker == 'ADA/USDT':
            amount = 200

        for order in self.timeout_orders: #이전 주문 관리
            if order['ticker'] == ticker:
                status = self.binanceObj.fetch_order_status(order['info']['orderId'], order['ticker'])
                if status == 'closed' and order['algo_side'] == 'enter':# exit주문
                    if pos_direction == 0:
                        exit_order = self.binanceObj.create_order(ticker, 'limit', 'sell', amount, exit_price)
                    else:
                        exit_order = self.binanceObj.create_order(ticker, 'limit', 'buy', amount, exit_price)
                    exit_order['ticker'] = ticker
                    exit_order['algo'] = algo
                    exit_order['algo_side'] = 'exit'
                    self.timeout_orders.append(exit_order)
                    print('@알고리즘:', algo, '티커:', ticker, '롱/숏:', pos_direction, '익절가격:', exit_price)
                    self.timeout_orders.remove(order)
                    return 0
                elif status == 'closed' and order['algo_side'] == 'exit':
                    return 0
                elif status =='open' :#체결 안된 것들 주문 취소
                    print("취소주문", ticker)
                    self.binanceObj.cancel_order(order['info']['orderId'], order['ticker'])
                    self.timeout_orders.remove(order)

        if pos_direction ==0:
            enter_order = self.binanceObj.create_order(ticker, 'limit', 'buy', amount, enter_price)
        else:
            enter_order = self.binanceObj.create_order(ticker, 'limit', 'sell', amount, enter_price)
        enter_order['algo'] = algo
        enter_order['algo_side'] = 'enter'
        enter_order['ticker'] = ticker
        self.timeout_orders.append(enter_order)
        print('@알고리즘:',algo,'티커:',ticker, '롱/숏:',pos_direction,'진입가격:',enter_price)

    def check_orders(self):
        print("===============================CHECK ORDERS=====================================")
        [print("timeout_orders:",order['algo'], order['ticker'], order['algo_side']) for order in self.timeout_orders]
        for order in self.order_list:
            status = self.binanceObj.fetch_order_status(order['info']['orderId'], order['ticker'])
            side = order['algo_side']
            algo = order['algo']#같은 전략
            ticker = order['ticker']
            print(algo, ticker, side, status)
            if status == 'closed' and side == 'stop_market':#시나리오1(드문 상황) 지정가 매수 후 급락 ->stop_market 까지 체결. 손절로 마무리.
                #손절 주문 체결
                # context.bot.send_message(chat_id=update.effective_chat.id, text = "손절 주문이 체결되었습니다.")
                for o in self.order_list:
                    if algo == o['algo']: #order_list에서 stop_market제거 후 다음 인덱스인 enter도 제거
                        index = self.order_list.index(o)
                        self.order_list.remove(o)
                        self.order_list.pop(index)
            if status == 'closed' and side == 'enter':#시나리오2(일반적인 상황) 지정가 매수 까지 체결 -> 지정가 매수 제거 후 지정가 매도 주문
                #진입 주문 체결
                # context.bot.send_message(chat_id=update.effective_chat.id, text = "진입 주문이 체결되었습니다.")
                index_num = 0
                for o in self.order_list:
                    stat = self.binanceObj.fetch_order_status(o['info']['orderId'], o['ticker'])
                    if algo == o['algo'] and stat == 'closed':
                        index_num = self.order_list.index(o)
                        self.order_list.remove(o)
                for s in self.stand_by_orders:
                    if algo == s['algo'] and ticker == s['ticker']:
                        if s['direction'] ==0: exit_order = self.binanceObj.create_order(s['ticker'], 'limit', 'sell', s['amount'], s['price'])#롱 일때
                        else: exit_order = self.binanceObj.create_order(s['ticker'], 'limit', 'buy', s['amount'], s['price'])#숏 일때
                        exit_order['algo'] = algo
                        exit_order['algo_side'] = 'exit'
                        exit_order['ticker'] = s['ticker']
                        # self.order_list.append(exit_order)
                        self.order_list.insert(index_num, exit_order)
                        print("포지션 진입 후 익절 주문 완료")
                        self.stand_by_orders.remove(s)
            if status == 'closed' and side =='exit':#지정가 매수 후 지정가 매도까지 체결 -> 지정가 주문
                for o in self.order_list:
                    # 익절 주문 체결
                    # context.bot.send_message(chat_id=update.effective_chat.id, text = "익절 주문이 체결되었습니다.")
                    if algo == o['algo'] and o['algo_side'] == 'stop_market':#stop_market이 미체결인 경우
                        self.binanceObj.cancel_order(o['info']['orderId'], o['ticker'])#주문 취소
                        self.order_list.remove(o)#stop_market 리스트에서 제거
                self.order_list.remove(order)#지정가 매도주문도 리스트에서 제거


    def are_algos_working(self):
        stObj = Stretegy()
        stand_by_algo_set = set()
        all_algo_set = set(['algo2_pre_low', 'algo3_pre_high'])
        for order in self.order_list:
            stand_by_algo_set.add(order['algo'])
        not_working_algo = all_algo_set - stand_by_algo_set
        for i in not_working_algo:
            if i == 'algo2_pre_low':
                print("algo#2 진입")
                # for t in self.execute_table:
                #     stObj.algo2_pre_low(t[1], t[3])
            if i == 'algo3_pre_high':
                print("algo#3 진입")
                for t in self.execute_table:
                    stObj.algo3_pre_high(t[1], t[3])

    def main(self):
        self.clear_realtime_table()
        for tf in self.execute_table:  # 각 분봉, 시간봉, 일봉 당 최근 100개의 데이터 mysql에 저장
            self.ohlcv_to_mysql(tf[1], tf[0], tf[3])
        for tf in self.execute_table:  # 최근 시간 지표 셋팅
            self.setting_indicator(tf[1])
        print("================================================================================")
        stObj = Stretegy()
        schedule.every().minutes.at(":07").do(self.is_it_late_data, tf_table = self.execute_table)
        schedule.every().minutes.at(":20").do(self.are_algos_working)
        schedule.every().minutes.at(":30").do(self.check_orders)
        schedule.every().minutes.at(":00").do(self.check_orders)
        # schedule.every(20).seconds.do(self.check_orders)
        # schedule.every(1).minutes.do(self.are_algos_working)

class Stretegy(Binance):
    def __init__(self):
        super().__init__()
    def screen_low(self, table, id, count, range):
        sql = '''SELECT *FROM `{0}` WHERE low = (SELECT MIN(low)FROM `{0}` WHERE id>{1}-{2} and id<={1});'''\
            .format(table, id, range)
        cursor.execute(sql)
        low_point = cursor.fetchall()
        if id == low_point[0]['id']:  # 최 우측이 low일 때
            if count == 0:  # 첫 번째 스크리닝 이면 range를 2배로 늘림
                return self.screen_low(table, id, count + 1, range+10)
            else:  # low 채택
                return low_point[0]['low']
        else:
            return self.screen_low(table, low_point[0]['id'], count + 1, 5)
    def screen_high(self, table, id, count, range):
        sql = '''SELECT *FROM `{0}` WHERE high = (SELECT MAX(high)FROM `{0}` WHERE id>{1}-{2} and id<={1});''' \
            .format(table, id, range)
        cursor.execute(sql)
        high_point = cursor.fetchall()
        if id == high_point[0]['id']:  # 최 우측이 low이면
            if count == 0:  # 첫 번째 스크리닝시 직전 캔들이 저점이면 직전 저점보다 더 낮은 캔들을 찾는다.
                return self.screen_high(table, id, count + 1, range+10)
            else:  # low 채택
                return high_point[0]['high']
        else:
            return self.screen_high(table, high_point[0]['id'], count + 1, 5)

    def algo2_pre_low(self, table, ticker):
        sql = '''SELECT * FROM `{0}` ORDER BY id DESC LIMIT 1'''.format(table)
        cursor.execute(sql)
        result = cursor.fetchall()
        enter_price = self.screen_low(table, result[0]['id'], 0, 10)
        exit_price = float(enter_price) * 1.005  # 0.5% 단타
        stop_market = float(enter_price) * 0.995  # 0.5% 손절
        print("@티커: "+ticker+" 매수진입: " + str(enter_price) + ",  익절가: " + str(exit_price) + ",  손절가: " + str(stop_market))
        self.enter_position(enter_price, exit_price, stop_market, 'algo2_pre_low', 0, ticker)

    def algo3_pre_high(self, table, ticker):
        sql = '''SELECT * FROM `{0}` ORDER BY id DESC LIMIT 1'''.format(table)
        cursor.execute(sql)
        result = cursor.fetchall()
        enter_price = self.screen_high(table, result[0]['id'], 0, 10)
        exit_price = float(enter_price) * 0.995  # 0.5% 단타
        stop_market = float(enter_price) * 1.005  # 0.5% 손절
        print("@티커: "+ticker+" 매도진입: " + str(enter_price) + ",  수익 실현 가격: " + str(
            exit_price) + ",  손절 가격: " + str(stop_market))
        self.enter_position(enter_price, exit_price, stop_market, 'algo3_pre_high', 1, ticker)

    def algo4_bollinger_band(self, table, ticker):
        sql = '''SELECT * FROM `{0}` ORDER BY id DESC;'''.format(table)
        cursor.execute(sql)
        result = cursor.fetchall()
        direction, enter_price, stop_market, exit_price = 0,0,0,0
        upper = result[0]['upperBB']
        lower = result[0]['lowerBB']
        for i in range(len(result)-1):
            if result[i]['high'] >= result[i]['upperBB']:
                direction = 0
                enter_price = lower
                stop_market = lower * 0.995
                exit_price = (3 * upper + lower) / 4  # 밴드75%에서 익절
            elif result[i]['low'] <= result[i]['lowerBB']:  # 밴드 하단 터치 -> 다음 포지션 숏
                direction = 1
                enter_price = upper
                stop_market = upper * 1.005
                exit_price = (upper + 3 * lower) / 4  # 밴드25%에서 익절

        # if (upper-lower)/lower*100<=0.5:#상단과 하단갭이 0.5%보다 작으면
        #     return
        self.timeout_position(enter_price, exit_price, stop_market, 'algo4_bollinger_band', direction, ticker)


if __name__ == "__main__":
    BObj = Binance()
    BObj.main()
    while True:
        schedule.run_pending()