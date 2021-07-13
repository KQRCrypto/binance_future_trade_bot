# import main
# from main import *
#
# class Stretegy(Binance):
#     def __init__(self):
#         super().__init__()
#     def screening(self, table, id, count):
#         sql = '''SELECT *FROM `{0}` WHERE low = (SELECT MIN(low)FROM `{0}` WHERE id>{1}-4 and id<{1});'''.format(
#             table, id)
#         cursor.execute(sql)
#         result = cursor.fetchall()
#         if id - 1 == result[0]['id']:  # 최 우측이 low이면
#             if count == 0:  # 첫 번째 스크리닝 이면 전 캔들부터 시작
#                 return self.screening(table, id - 1, count + 1)
#             else:  # low 채택
#                 return result[0]['low']
#         else:
#             print(id)
#             return self.screening(table, result[0]['id'] + 1, count + 1)
#
#     def algo2_prelow(self, table):  # 직전 저점 제외, 최소 3캔들 이전
#         count = 0
#         sql = '''SELECT * FROM `{0}` ORDER BY id DESC LIMIT 1'''.format(table)
#         cursor.execute(sql)
#         result = cursor.fetchall()
#         buy_limit = self.screening(table, result[0]['id'], count)
#         sell_limit = float(buy_limit) * 1.005  # 0.5% 단타
#         stop_market = float(buy_limit) * 0.995  # 0.5% 손절
#         cur_price = self.binanceObj.fetch_ticker("BTC/USDT")
#         print("현재가: "+str(cur_price['close'])+" 지정가 매수: " + str(buy_limit) + ",  익절가: " + str(sell_limit) + ",  손절가: " + str(stop_market))
#         self.normal_enter_position(buy_limit, sell_limit, stop_market, 'algo2_prelow')
#
