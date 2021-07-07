# import ccxt
# import pandas as pd
# from datetime import datetime
# import time
# import pymysql
# import schedule
#
# with open("../api.txt")as f:
#     lines = f.readlines()
#     api_key = lines[0].strip()
#     secret = lines[1].strip()
#     db = lines[2].strip()
#     password = lines[3].strip()
# db = pymysql.connect(
#     user='root',
#     passwd=password,
#     host='127.0.0.1',
#     db=db,
#     charset='utf8'
# )
# cursor = db.cursor(pymysql.cursors.DictCursor)
#
# class Binance:
#     #선물거래용 바이낸스 객체 생성
#     def __init__(self):
#         self.binanceObj = ccxt.binance(config={
#             'apiKey': api_key,
#             'secret': secret,
#             'enableRateLimit' : True,
#             'options':{
#                 'defaultType':'future'
#             }
#         })
#         self.tf_table = [['1m', 'realtime_btc_minute', 1], ['5m', 'realtime_btc_5minute', 5], ['15m', 'realtime_btc_15minute', 15],
#                     ['1h', 'realtime_btc_hour', 60], ['4h', 'realtime_btc_4hour', 240], ['1d', 'realtime_btc_day', 3600]]  # 1열은 timeframe, 2열은 테이블 명
#
#     def timestamp_to_str(self, time): # 타임스탬프 문자열로 변환
#         return str(time.year*100000000 + time.month*1000000 + time.day*10000 + time.hour*100 + time.minute)
#
#     def clear_realtime_table(self):
#         for tf in self.tf_table:#realtime 전체 테이블 삭제
#             sql = '''TRUNCATE `{0}`'''.format(tf[1])
#             cursor.execute(sql)
#             db.commit()
#         print("전체 realtime_table 초기화 완료")
#         print("================================================================================")
#     def minute_to_mysql(self, table, timeframe):
#         btc_ohlcv = self.binanceObj.fetch_ohlcv("BTC/USDT", timeframe=timeframe,
#                                                 limit=100)  # 1번 반복이 한시간 +3600000 24시간 +86400000
#         for i in range(len(btc_ohlcv)-1):
#             stamp = pd.to_datetime(btc_ohlcv[i][0] * 1000000)
#             times = self.timestamp_to_str(stamp)  # MySQL에는 UTC시간으로 저장
#             Range = (btc_ohlcv[i][2] - btc_ohlcv[i][3]).__round__(2)
#             sql = '''INSERT INTO `{0}` (time, open, high, low, close, volume, ranges) VALUES({1}, {2}, {3}, {4}, {5}, {6}, {7})'''.\
#                 format(table, times, btc_ohlcv[i][1], btc_ohlcv[i][2], btc_ohlcv[i][3],
#                                                                btc_ohlcv[i][4], btc_ohlcv[i][5], Range)
#             cursor.execute(sql)
#         db.commit()
#         time.sleep(0.1)
#
#     def setting_indicator(self, table):  # OHLCV기반으로 지표 생성 후 DB 테이블 업데이트
#         sql = '''SELECT * FROM {0}'''.format(table)
#         cursor.execute(sql)
#         result = cursor.fetchall()
#         df = pd.DataFrame(result)
#         df['MA7'] = df['close'].rolling(window=7, min_periods=1).mean()
#         df['MA25'] = df['close'].rolling(window=25, min_periods=1).mean()
#         df['MA99'] = df['close'].rolling(window=99, min_periods=1).mean()
#         df['stddev'] = df['close'].rolling(window=25, min_periods=1).std()
#         df['upperBB'] = df['MA25'] + df['stddev'] * 2
#         df['lowerBB'] = df['MA25'] - df['stddev'] * 2
#         for i in range(1, len(df)):  # 마지막 행만 지표가 있으면 된다.
#             sql = '''UPDATE `{0}` SET MA7 = {2}, MA25 = {3}, MA99 = {4}, upperBB = {5}, lowerBB={6} WHERE id ={1}'''.format(
#                 table, df.loc[i]['id'], df.loc[i]['MA7'], df.loc[i]['MA25'], df.loc[i]['MA99'], df.loc[i]['upperBB'],
#                 df.loc[i]['lowerBB'])
#             # sql = '''UPDATE `{0}` SET MA7 = {2}, MA25 = {3}, MA99 = {4}, upperBB = {5}, lowerBB={6} WHERE id ={1}'''.format(table, df.loc[i]['id'], df.loc[i]['MA7'], df.loc[i]['MA25'], df.loc[i]['MA99'])
#             cursor.execute(sql)
#         db.commit()
#         print(table + ": 완료")
#
#
#     def load_last_time(self, table):#mysql에 저장되있는 마지막 시간대 불러오기
#         sql = '''SELECT * FROM `{0}` ORDER BY id DESC LIMIT 1'''.format(table)
#         cursor.execute(sql)
#         result = cursor.fetchall()
#         return result[0]['id'], result[0]['time']
#
#     def update_indicator(self, table, diff):  # OHLCV기반으로 지표 생성 후 DB 테이블 업데이트
#         sql = '''SELECT * FROM {0}'''.format(table)
#         cursor.execute(sql)
#         result = cursor.fetchall()
#         df = pd.DataFrame(result)
#         df['MA7'] = df['close'].rolling(window=7, min_periods=1).mean()
#         df['MA25'] = df['close'].rolling(window=25, min_periods=1).mean()
#         df['MA99'] = df['close'].rolling(window=99, min_periods=1).mean()
#         df['stddev'] = df['close'].rolling(window=25, min_periods=1).std()
#         df['upperBB'] = df['MA25'] + df['stddev'] * 2
#         df['lowerBB'] = df['MA25'] - df['stddev'] * 2
#         diff = 1
#         for i in range(len(df) - diff, len(df)):  # 마지막 행만 지표가 있으면 된다.
#             sql = '''UPDATE `{0}` SET MA7 = {2}, MA25 = {3}, MA99 = {4}, upperBB = {5}, lowerBB={6} WHERE id ={1}'''.format(
#                 table, df.loc[i]['id'], df.loc[i]['MA7'], df.loc[i]['MA25'], df.loc[i]['MA99'], df.loc[i]['upperBB'],
#                 df.loc[i]['lowerBB'])
#             # sql = '''UPDATE `{0}` SET MA7 = {2}, MA25 = {3}, MA99 = {4}, upperBB = {5}, lowerBB={6} WHERE id ={1}'''.format(table, df.loc[i]['id'], df.loc[i]['MA7'], df.loc[i]['MA25'], df.loc[i]['MA99'])
#             cursor.execute(sql)
#         db.commit()
#         print(table + ": 지표 업데이트 완료")
#
#     def algo1_larry(self, table):  # mysql에 저장되있는 마지막 시간대 불러오기
#         sql = '''SELECT * FROM `{0}` ORDER BaY id DESC LIMIT 2'''.format(table)
#         cursor.execute(sql)
#         result = cursor.fetchall()
#         range = (result[1]['high'] - result[1]['low'])
#         long_target = result[0]['open'] + range
#         short_target = result[0]['open'] - range
#         print("long target: "+str(long_target)+",  short target: "+str(short_target))
#         self.enter_position(long_target, short_target)
#         # return long_target, short_target
#
#     def enter_position(self, long_target, short_target):  # 15분에 한번씩 실행 -> 15분봉 업데이트 될 때 실행
#         symbol = 'BTC/USDT'
#         amount = 0.001
#         buy_order = self.binanceObj.create_order(symbol, 'STOP', 'buy', amount, long_target,
#                                             params={'stopPrice': long_target})
#         sell_order = self.binanceObj.create_order(symbol, 'STOP', 'sell', amount, short_target,
#                                              params={'stopPrice': short_target})
#         print("스탑리밋 주문 완료")
#         return buy_order, sell_order
#
#         self.binanceObj.cancel_all_orders('BTC/USDT')
#     def exit_position(self):
#         balance = self.binanceObj.fetch_balance()
#         self.binanceObj.cancel_all_orders('BTC/USDT')
#         position = float(balance['info']['positions'][101]['positionAmt'])
#         if position > 0:
#             self.binanceObj.create_market_sell_order('BTC/USDT', abs(position))
#             print("포지션 반대매매 완료")
#         elif position < 0:
#             self.binanceObj.create_market_buy_order('BTC/USDT', abs(position))
#             print("포지션 반대매매 완료")
#         else:
#             print("보유중이던 포지션이 없습니다.")
#         self.algo1_larry(self.tf_table[1][1])
#
#     def is_it_late_data(self, tf_table):
#         for tf in tf_table:
#             law = self.load_last_time(tf[1])[1]
#             t = law[0:4] + '-' + law[4:6] + '-' + law[6:8] + ' ' + str(int(law[8:10])) + ':' + law[10:12] + ':' + law[12:14] + '00'  # 원형 :'2021-01-01 09:00:00'. 시간은 UTC기준이므로 +9시간
#             late_time = int(time.mktime(datetime.strptime(t, '%Y-%m-%d %H:%M:%S').timetuple()) * 1000)  # 처음 데이터 가져올 때
#             cur_time = self.binanceObj.fetch_ohlcv("BTC/USDT", timeframe=tf[0], limit=1)[0][0] - 32400000
#             # print("최신 업데이트 후 "+str((cur_time - late_time)/1000)+"초 경과 했습니다."+str(tf[2])+" 봉 업데이트 심사")
#             #왜 6만차이야? 12만 차이 나야하는거 아냐?
#             # print(cur_time - late_time)
#             if cur_time - late_time == 60000:
#                 print("바이낸스 딜레이로 인한 문제 발생")
#             if cur_time - 60000 * tf[2] <= late_time:
#                 print(tf[1]+"는 최신 데이터이므로 업데이트하지 않습니다.")
#                 break
#             else:
#                 print(tf[1]+"에 최신 데이터를 추가 저장 합니다.")
#                 diff = int((cur_time - late_time) / 60000)  # 가장 최근에 저장한 데이터는 봉 마감전 데이터므로 다시 업데이트 한다.
#                 btc_ohlcv = self.binanceObj.fetch_ohlcv("BTC/USDT", timeframe=tf[0], limit=2)
#                 #여기서 for문
#                 stamp = pd.to_datetime(btc_ohlcv[0][0] * 1000000)
#                 times = self.timestamp_to_str(stamp)  # MySQL에는 UTC시간으로 저장
#                 Range = (btc_ohlcv[0][2] - btc_ohlcv[0][3]).__round__(2)
#                 sql = '''INSERT INTO `{0}` (time, open, high, low, close, volume, ranges)
#                     VALUES({1}, {2}, {3}, {4}, {5}, {6}, {7})'''.format(tf[1], times, btc_ohlcv[0][1], btc_ohlcv[0][2],
#                                                                    btc_ohlcv[0][3],
#                                                                    btc_ohlcv[0][4], btc_ohlcv[0][5], Range)
#                 cursor.execute(sql)
#                 db.commit()
#                 # for i in range(len(btc_ohlcv)):
#                 #     stamp = pd.to_datetime(btc_ohlcv[i][0] * 1000000)
#                 #     times = self.timestamp_to_str(stamp)  # MySQL에는 UTC시간으로 저장
#                 #     if i == 0:  # 가장 최근에 저장한 데이터는 봉 마감전 데이터므로 다시 업데이트 한다.
#                 #         sql = '''UPDATE `{0}` SET open={1}, high={2}, low={3}, close={4}, volume={5} WHERE id={6}'''.format(
#                 #             tf[1], btc_ohlcv[i][1], btc_ohlcv[i][2], btc_ohlcv[i][3],
#                 #             btc_ohlcv[i][4], btc_ohlcv[i][5], id)
#                 #         cursor.execute(sql)
#                 #     else:
#                 #         sql = '''INSERT INTO `{6}` (time, open, high, low, close, volume)
#                 #             VALUES({0}, {1}, {2}, {3}, {4}, {5})'''.format(times, btc_ohlcv[i][1], btc_ohlcv[i][2],
#                 #                                                            btc_ohlcv[i][3],
#                 #                                                            btc_ohlcv[i][4], btc_ohlcv[i][5], tf[1])
#                 #         cursor.execute(sql)
#                 # db.commit()
#                 # print("최신화 완료")
#                 self.update_indicator(tf[1], diff)
#                 if tf[2] == 15: #15분봉 업데이트 시 변동성 돌파전략
#                     print("check")
#                     self.exit_position()
#
#         print("현재시각: "+str(datetime.now()))
#         print("================================================================================")
#
#
#
#     def main(self):
#         self.clear_realtime_table()
#         for tf in self.tf_table:  # 각 분봉, 시간봉, 일봉 당 최근 100개의 데이터 mysql에 저장
#             self.minute_to_mysql(tf[1], tf[0])
#         for tf in self.tf_table:  # 최근 시간 지표 셋팅
#             self.setting_indicator(tf[1])
#         print("================================================================================")
#         schedule.every().minutes.at(":10").do(self.is_it_late_data, tf_table = self.tf_table)
#
#         # btc_ohlcv = self.binanceObj.fetch_ohlcv("BTC/USDT", timeframe='1m', limit=2)
#         # print(btc_ohlcv)
# if __name__ == "__main__":
#     BObj = Binance()
#     BObj.main()
#     while True:
#         # schedule.every(10).seconds.do(is_it_late_data(tf_table))
#         schedule.run_pending()
