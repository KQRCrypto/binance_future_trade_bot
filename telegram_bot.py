
from re import T
from ccxt.base import precise
import telegram
import ccxt
import pymysql
import pprint

from telegram import Update
from telegram.ext import Updater,CommandHandler,CallbackContext,MessageHandler

with open("../api.txt")as f:
    lines = f.readlines()
    api_key = lines[0].strip()
    secret = lines[1].strip()
    db = lines[2].strip()
    password = lines[3].strip()
    api_code = lines[4].strip()
    id = lines[5].strip()
db = pymysql.connect(
    user='root',
    passwd=password,
    host='127.0.0.1',
    db=db,
    charset='utf8'
)
cursor = db.cursor(pymysql.cursors.DictCursor)

binance = ccxt.binance(config={
    'apiKey': api_key,
    'secret' :secret
})

binance_f = ccxt.binance(config={ # for future trade 
    'apiKey': api_key,
    'secret': secret,
    'enableRateLimit': True,
    'options':{
        'defaultType':'future'
    }
})

# info for bot 
bot = telegram.Bot(token=api_code)
updates = bot.getUpdates()
chat_id = id

# to cancel
latest_order_id = ""
latest_order_symbol = ""

# updater, dispatcher 
updater = Updater(token=api_code, use_context=True)
dispatcher = updater.dispatcher
print("run")

# /start 
def start(update,context):
    context.bot.send_message(chat_id=update.effective_chat.id, text = 
    "command\n"
    "/balance s: spot/future 지갑 잔고 조회\n"
    "/market BTC: 마켓에서 BTC 코드 조회\n" #주문 넣을 때 마켓코드이용
    "/order BTC/USDT s b 50000 0.1: symbol spot/future buy/sell price num\n"
    "/cancel: cancel latest order"
    "/price m (or d): 분봉/일봉 조회\n"
    "/stop: stop chat\n")
    

# 계좌정보 불러오기 - 현물 지갑 잔고 조회, 선물 지갑 잔고 조회  
# 차트 불러오기- 현재가 조회
# 매수 매도 테스트 - 매수 주문, 매도 주문(지정가 주문), 포지션 


def stop():
    updater.stop()

def balance(update,context):
    # 잔고, 보유 상황, 미체결 내역
    t = update.message.text
    print(t)
    if t.split()[1] == "s":
        balance = binance.fetch_balance()
        result = "spot:"+str(balance['USDT'])
        context.bot.send_message(chat_id=update.effective_chat.id, text=result)
    elif t.split()[1] =="f":
        balance = binance.fetch_balance(params={"type":"future"}) #future
        result = "future:"+str(balance['USDT'])
        context.bot.send_message(chat_id=update.effective_chat.id, text=result)


def market(update,context):
    t = update.message.text
    markets = binance.load_markets()
    for market in markets.keys():
        if market.startswith(t.split()[1]):
            context.bot.send_message(chat_id=update.effective_chat.id, text=str(market))

        
def order(update,context):
    # 잔고 조회 후 가격 미만이면 매수 실패 매도 실패 
    # 주문 체결 후 메세지  
    # /order BTC/USDT s b 50000 0.1: spot/future buy/sell 가격 수량
    t = update.message.text
    print(t)
    if t.split()[2] == 's': #현물거래 
        if t.split()[3] == 'b': # 매수
            order = binance.create_limit_buy_order(
                symbol= t.split()[1],
                amount= t.split()[5],
                price= t.split()[4]
            )
            pprint.pprint(order)
            latest_order_id = order['info']['orderId']
            latest_order_symbol = order['symbol']
            context.bot.send_message(chat_id=update.effective_chat.id, text = "매수하였습니다.")
        elif t.split()[3] == 's': # 매도
            order = binance.create_limit_sell_order(
                symbol= t.split()[1],
                amount= t.split()[5],
                price= t.split()[4]
            )
            pprint.pprint(order)
            latest_order_id = order['info']['orderId']
            latest_order_symbol = order['symbol']
            context.bot.send_message(chat_id=update.effective_chat.id, text = "매도하였습니다.")
    #elif t.split()[2] == 'f': # 선물 거래 
        
   
def cancel(update,context):
    ret = binance.cancel_order(
        id = latest_order_id,
        symbol = latest_order_symbol
    )
    print(ret) 
    context.bot.send_message(chat_id=update.effective_chat.id, text = "주문을 취소하였습니다.")

def price(update,context):
    context.bot.send_message(chat_id=update.effective_chat.id, text = "데이터 : ")

def warning(update,context):
    # 봇이 문제가 생길 경우 가동 중단
    context.bot.send_message(chat_id=update.effective_chat.id, text = "봇 가동을 중단합니다.")
    updater.stop()


# handler, dispatcher
start_handler = CommandHandler('start',start)
dispatcher.add_handler(start_handler) # start command를 요청하면 start function 실행

stop_handler = CommandHandler('stop',stop)
dispatcher.add_handler(stop_handler) 

balance_handler = CommandHandler('balance',balance)
dispatcher.add_handler(balance_handler) 

market_handler = CommandHandler('market',market)
dispatcher.add_handler(market_handler) 

order_handler = CommandHandler('order',order)
dispatcher.add_handler(order_handler) 

cancel_handler = CommandHandler('cancel',cancel)
dispatcher.add_handler(cancel_handler) 

price_handler = CommandHandler('price',price)
dispatcher.add_handler(price_handler) 

warning_handler = CommandHandler('warning',warning)
dispatcher.add_handler(warning_handler) 

try:
    updater.start_polling() # 코드가 종료되지 않고 계속 수행
except:
    warning()



















