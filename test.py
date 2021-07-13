import main
import pandas as pd
from datetime import datetime
import time
import ccxt
import pymysql
import matplotlib.pyplot as plt
import schedule
import pprint
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
tf_table = [['1m', 'realtime_btc_minute', 1], ['15m', 'realtime_btc_15minute', 15], ['5m', 'realtime_btc_5minute', 5],
             ['1h', 'realtime_btc_hour', 60], ['4h', 'realtime_btc_4hour', 240], ['1d', 'realtime_btc_day', 3600]]#1열은 timeframe, 2열은 테이블 명

tf_table.remove(['1m', 'realtime_btc_minute', 1])

symbol = 'BTC/USDT'
balance = binanceObj.fetch_balance()
balance['info']['assets'][1]['availableBalance']#사용가능한 USDT잔액
pprint.pprint()

orders = binanceObj.fetch_orders('BTC/USDT')
orders = binanceObj.fetch_orders(id='24814336293')
binanceObj.fetch_order_status(24814684730, 'BTC/USDT')
orders[0]['info']
orders[1]['info']
float(balance['info']['positions'][101]['positionAmt']) != 0
positionAmt = float(balance['info']['positions'][101]['positionAmt'])
entry_price = balance['info']['positions'][101]['entryPrice']
pprint.pprint(balance)
print(balance['USDT'])
li = []
li.append(binanceObj.create_market_sell_order('BTC/USDT', 0.001))#
li.append(binanceObj.create_order('BTC/USDT','limit','buy', 0.001, 34000))#지정가 주문
a = li[0]['info']['orderId']
binanceObj.fetch_order(a, 'BTC/USDT')['amount']
binanceObj.fetch_order(a, 'BTC/USDT')['side']
binanceObj.fetch_order(a, 'BTC/USDT')['info']
binanceObj.fetch_order_status(a, 'BTC/USDT')

binanceObj.cancel_order(a, 'BTC/USDT')#주문취소
li.pop()
li.append({'symbol' : 'BTC/USDT'})
li.remove({'symbol' : 'BTC/USDT'})

buy_order = binanceObj.create_order(symbol, 'STOP', 'buy', 0.001, 33380, params={'stopPrice': 33380})
buy_order['info']
'''{'orderId': '24814336293', 'symbol': 'BTCUSDT', 'status': 'NEW', 'clientOrderId': 'x-xcKtGhcu448c451d5bf89c304552d', 'price': '33370', 'avgPrice': '0.00000', 'origQty': '0.001', 'executedQty': '0', 'cumQty': '0', 'cumQuote': '0', 'timeInForce': 'GTC', 'type': 'STOP', 'reduceOnly': False, 'closePosition': False, 'side': 'BUY', 'positionSide': 'BOTH', 'stopPrice': '33370', 'workingType': 'CONTRACT_PRICE', 'priceProtect': False, 'origType': 'STOP', 'updateTime': '1625249151429'}'''
for i in range(len(balance['info']['positions'])):
    print(balance['info']['positions'][i]['symbol'], i)



# sell_order = binanceObj.create_order('BTC/USDT', 'STOP', 'sell', 0.01, 33780, params={'stopPrice':33780})
# order_id = sell_order['info']['orderId']
# binanceObj.fetch_order_status(order_id, 'BTC/USDT')


#지표 가져오기
result_list = []
def bring_indicator(tf_table):
    btc = binanceObj.fetch_ticker("BTC/USDT")
    cur_price = btc['last']
    for tf in tf_table:
        sql = '''SELECT * FROM `{0}` ORDER BY id DESC LIMIT 99'''.format(tf[1])
        cursor.execute(sql)
        result = cursor.fetchall()
        result = pd.DataFrame(result)
        result['bandwidth'] = (result.upperBB - result.lowerBB)/cur_price*100
        result['percentBand'] = (cur_price - result.lowerBB)/(result.upperBB - result.lowerBB)
        result_list.append(result)
bring_indicator(tf_table)
# plot


def visualization():
    for i in range(4):
        plt.subplot(411+i)
        df = result_list[i]
        plt.plot(df.id, df['close'], color='green', label='close')
        plt.plot(df.id, df['MA25'], 'k--', label='MA25')
        plt.plot(df.id, df.upperBB, 'r--', label='upper')
        plt.plot(df.id, df.lowerBB, 'b--', label='lower')
        plt.fill_between(df.id, df.upperBB, df.lowerBB, color='0.95')
        for i in range(len(df.id)):
            if df.close[i]*0.995<=df.lowerBB[i]:
            # if df.percentBand[i] > 0 and df.percentBand[i] < 0.2:
                    plt.plot(df.id[i], df.close[i], 'r^')
            elif df.close[i]*1.005>=df.upperBB[i]:
            # elif df.percentBand[i] > 0.8 and df.percentBand[i] < 1:
                plt.plot(df.id[i], df.close[i], 'bv')
        plt.grid(True)
        plt.ylim([min(df.lowerBB), max(df.upperBB)])
        plt.legend(loc='best')
plt.figure(figsize=(10, 10))
visualization()
plt.show()

