import ccxt
# import pandas as pd
# from datetime import datetime
# import time
import pymysql
import schedule
from Setting import Setting
import telegram_bot

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
        print("주문 완료")
        enter_order['algo'] = algo
        enter_order['algo_side'] = 'enter'
        enter_order['ticker'] = ticker
        stop_order['algo'] = algo
        stop_order['algo_side'] = 'stop_market'
        stop_order['ticker'] = ticker
        self.stand_by_orders.append({'algo':algo, 'price':exit_price, 'amount':amount, 'direction':pos_direction, 'ticker':ticker})
        self.order_list.append(stop_order) # stop_market먼저 order_list에 추가
        self.order_list.append(enter_order)


    def check_orders(self,update,context):
        print("===============================CHECK ORDERS=====================================")
        for order in self.order_list:
            status = self.binanceObj.fetch_order_status(order['info']['orderId'], order['ticker'])
            side = order['algo_side']
            algo = order['algo']#같은 전략
            ticker = order['ticker']
            print(algo, ticker, side, status)
            if status == 'closed' and side == 'stop_market':#시나리오1(드문 상황) 지정가 매수 후 급락 ->stop_market 까지 체결. 손절로 마무리.
                ###############
                #손절 주문 체결##
                context.bot.send_message(chat_id=update.effective_chat.id, text = "손절 주문이 체결되었습니다.")
                ###############
                for o in self.order_list:
                    if algo == o['algo']: #order_list에서 stop_market제거 후 다음 인덱스인 enter도 제거
                        index = self.order_list.index(o)
                        self.order_list.remove(o)
                        self.order_list.pop(index)
            if status == 'closed' and side == 'enter':#시나리오2(일반적인 상황) 지정가 매수 까지 체결 -> 지정가 매수 제거 후 지정가 매도 주문
                ###############
                #진입 주문 체결##
                context.bot.send_message(chat_id=update.effective_chat.id, text = "진입 주문이 체결되었습니다.")
                ###############
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
                    ###############
                    # 익절 주문 체결##
                    context.bot.send_message(chat_id=update.effective_chat.id, text = "익절 주문이 체결되었습니다.")
                    ###############
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
            # if i == 'algo2_pre_low':
                # print("algo#2 진입")
                # for t in self.execute_table:
                    # stObj.algo2_pre_low(t[1], t[3])
            if i == 'algo3_pre_high':
                print("algo#3 진입")
                for t in self.execute_table:
                    stObj.algo3_pre_high(t[1], t[3])
                    # stObj.algo3_pre_high('realtime_btc_15minute', 'BTC/USDT')
                    # stObj.algo3_pre_high('realtime_eth_15minute', 'ETH/USDT')
                    # stObj.algo3_pre_high('realtime_ada_hour', 'ADA/USDT')


    def main(self):
        self.clear_realtime_table()
        for tf in self.execute_table:  # 각 분봉, 시간봉, 일봉 당 최근 100개의 데이터 mysql에 저장
            self.ohlcv_to_mysql(tf[1], tf[0], tf[3])
        for tf in self.execute_table:  # 최근 시간 지표 셋팅
            self.setting_indicator(tf[1])
        print("================================================================================")
        schedule.every().minutes.at(":10").do(self.is_it_late_data, tf_table = self.execute_table)
        schedule.every().minutes.at(":20").do(self.check_orders)
        schedule.every().minutes.at(":50").do(self.check_orders)
        schedule.every().minutes.at(":00").do(self.are_algos_working)
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

    def algo2_pre_low(self, table, ticker):  # 직전 저점 제외, 최소 3캔들 이전
        sql = '''SELECT * FROM `{0}` ORDER BY id DESC LIMIT 1'''.format(table)
        cursor.execute(sql)
        result = cursor.fetchall()
        enter_price = self.screen_low(table, result[0]['id'], 0, 10)
        exit_price = float(enter_price) * 1.005  # 0.5% 단타
        stop_market = float(enter_price) * 0.995  # 0.5% 손절
        print("티커: "+ticker+" 매수진입: " + str(enter_price) + ",  익절가: " + str(exit_price) + ",  손절가: " + str(stop_market))
        self.enter_position(enter_price, exit_price, stop_market, 'algo2_pre_low', 0, ticker)

    def algo3_pre_high(self, table, ticker):  # 직전 저점 제외, 최소 3캔들 이전
        sql = '''SELECT * FROM `{0}` ORDER BY id DESC LIMIT 1'''.format(table)
        cursor.execute(sql)
        result = cursor.fetchall()
        enter_price = self.screen_high(table, result[0]['id'], 0, 10)
        exit_price = float(enter_price) * 0.995  # 0.5% 단타
        stop_market = float(enter_price) * 1.005  # 0.5% 손절
        print("티커: "+ticker+" 매도진입: " + str(enter_price) + ",  수익 실현 가격: " + str(
            exit_price) + ",  손절 가격: " + str(stop_market))
        self.enter_position(enter_price, exit_price, stop_market, 'algo3_pre_high', 1, ticker)



if __name__ == "__main__":
    BObj = Binance()
    BObj.main()
    while True:
        schedule.run_pending()