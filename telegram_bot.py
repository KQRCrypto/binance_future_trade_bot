
from re import T
from ccxt.base import precise
import telegram
import ccxt
import pymysql

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

# 정보 입력 
bot = telegram.Bot(token=api_code)
updates = bot.getUpdates()
chat_id = bot.getUpdates()[-1].message.chat.id

# updater, dispatcher 
updater = Updater(token=api_code, use_context=True)
dispatcher = updater.dispatcher

# /start 
def start(update,context):
    context.bot.send_message(chat_id=update.effective_chat.id, text = 
    "command\n"
    "/balance s: spot/future 지갑 잔고 조회\n"
    "/order s b 50000 0.1: spot/future buy/sell 가격 수량\n"
    "/cancel: cancel latest order"
    "/price m (or d): 분봉/일봉 조회\n"
    "/stop: stop chat\n")
    

# 계좌정보 불러오기 - 현물 지갑 잔고 조회, 선물 지갑 잔고 조회  
# 차트 불러오기- 현재가 조회
# 매수 매도 테스트 - 매수 주문, 매도 주문(지정가 주문), 포지션 


def stop():
    updater.start_polling()

def balance(update,context):
    t = update.message.text
    if t == "s":
        balance = binance.fetch_balance()
        context.bot.send_message(chat_id=update.effective_chat.id, text = "spot")
    elif t =="f":
        balance = binance.fetch_balance(params={"type":"future"}) #future
        context.bot.send_message(chat_id=update.effective_chat.id, text = "future")
    

def order(update,context):
    # 잔고 조회 후 가격 미만이면 매수 실패 매도 실패 
    binance.create_market_buy_order(
        symbol="",
        amount="",
        price=""
    )
    context.bot.send_message(chat_id=update.effective_chat.id, text = "매수하였습니다.")
    binance.create_market_sell_order(
        symbol="",
        amount="",
        price=""
    )
    context.bot.send_message(chat_id=update.effective_chat.id, text = "매도하였습니다.")

def cancel(update,context):
    order_id = order['info']['orderId']
    symbol = order['symbol']
    ret = binance.cancel_order(
        id = order_id,
        symbol = symbol
    )
    ret 
    context.bot.send_message(chat_id=update.effective_chat.id, text = "주문을 취소하였습니다.")

def price(update,context):
    context.bot.send_message(chat_id=update.effective_chat.id, text = "데이터 : ")


# handler, dispatcher
start_handler = CommandHandler('start',start)
dispatcher.add_handler(start_handler) # start command를 요청하면 start function 실행

stop_handler = CommandHandler('stop',stop)
dispatcher.add_handler(stop_handler) 

balance_handler = CommandHandler('balance',balance)
dispatcher.add_handler(balance_handler) 

order_handler = CommandHandler('order',order)
dispatcher.add_handler(order_handler) 

cancel_handler = CommandHandler('cancel',cancel)
dispatcher.add_handler(cancel_handler) 

price_handler = CommandHandler('price',price)
dispatcher.add_handler(price_handler) 


updater.start_polling() # 코드가 종료되지 않고 계속 수행




















