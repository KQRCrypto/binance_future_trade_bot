
from re import T
from ccxt.base import precise
import telegram
import ccxt
import pymysql
import pprint
from Error import Error

from telegram import Update
from telegram.ext import Updater,CommandHandler,CallbackContext,MessageHandler\

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
    'secret' :secret,
    'enableRateLimit': True
})

binance_f = ccxt.binance(config={ # for future trade 
    'apiKey': api_key,
    'secret': secret,
    'enableRateLimit': True,
    'options':{
        'defaultType':'future'
    }
})

class Telegram(Error):
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
    print("bot run")

    # /start 
    def start(update,context):
        context.bot.send_message(chat_id=update.effective_chat.id, text = 
        "command\n"
        "/balance s: spot/future 지갑 잔고 조회\n"
        "/market s BTC: spot/future 마켓에서 BTC 코드 조회\n" #주문 넣을 때 마켓코드이용
        "/trade s: trade spot/future\n" # 현물 - 매수,매도 / 선물 - 롱/숏, 포지션 정리 
        "/cancel: cancel latest order\n"
        "/price s BTC/USDT: spot/future BTC/USDT 현재가 조회\n"
        "/check BTC/USDT: BTC 미체결 주문 확인\n"
        "/stop: stop chat\n")
        

    # 계좌정보 불러오기 - 현물 지갑 잔고 조회, 선물 지갑 잔고 조회  
    # 차트 불러오기- 현재가 조회
    # 매수 매도 테스트 - 매수 주문, 매도 주문(지정가 주문), 포지션 

    # 추가: 체결시 알림, 미체결 주문 조회, 미체결 주문 취소, 봇 중단-> 메세지 중단
    # 에러 알림 함수(프로그램 자체에 에러를 추가해서 멈추게하라)
    # 주문이 체결될때마다 체결 로그를 만들어야함 -> 체결로그대로 시각화할 수 있는 툴을 만들어라
    # -> 데이터시각화는 전략을 점검할 수 있도록, 이 전략이 어디서 매수/매도 되었고, 어떻게 이익을 얻었는지 이런걸 확인할 수 있게


    def stop(update,context):
        # 에러를 만들어서 아예 멈춰지도록 구현 
        context.bot.send_message(chat_id=update.effective_chat.id, text = "봇 가동을 중단합니다.")
        raise Error
        

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
        if t.split()[1] =='s':
            markets = binance.load_markets()
            for market in markets.keys():
                if market.startswith(t.split()[2]):
                    context.bot.send_message(chat_id=update.effective_chat.id, text=str(market))
        elif t.split()[1] =='f':
            markets = binance_f.load_markets()
            for market in markets.keys():
                if market.startswith(t.split()[2]):
                    context.bot.send_message(chat_id=update.effective_chat.id, text=str(market))


            
    def order_s(update,context):
        # 잔고 조회 후 가격 미만이면 매수 실패 매도 실패 
        # 주문 체결 후 메세지  
        t = update.message.text
        print(t)
        try:
            if t.split()[2] == 'b': # 매수
                order = binance.create_limit_buy_order(
                    symbol= t.split()[1],
                    amount= t.split()[4],
                    price= t.split()[3]
                )
                pprint.pprint(order)
                latest_order_id = order['info']['orderId']
                latest_order_symbol = order['symbol']
                context.bot.send_message(chat_id=update.effective_chat.id, text = "매수 주문이 완료되었습니다.")
            elif t.split()[2] == 's': # 매도
                order = binance.create_limit_sell_order(
                symbol= t.split()[1],
                    amount= t.split()[4],
                    price= t.split()[3]
                )
                pprint.pprint(order)
                latest_order_id = order['info']['orderId']
                latest_order_symbol = order['symbol']
                context.bot.send_message(chat_id=update.effective_chat.id, text = "매도 주문이 완료되었습니다.")
        except: 
            context.bot.send_message(chat_id=update.effective_chat.id, text = 
            "Your order has not been received. Please check below.\n"
            "1. Order sum must be greater than 10\n"
            "2. Make sure you have enough balance\n"
            "3. The transaction fee on Binance Exchange is 0.1%")


    def order_f(update,context):
        t = update.message.text
        print(t)
        try:
            if t.split()[2] == 'l':  
                # 롱 / 숏 포지션 정리 
                order = binance_f.create_limit_buy_order(
                    symbol = t.split()[1],
                    amount = t.split()[4],
                    price= t.split()[3]
                )
                pprint.pprint(order)
                latest_order_id = order['info']['orderId']
                latest_order_symbol = order['symbol']
                context.bot.send_message(chat_id=update.effective_chat.id, text = "주문이 완료되었습니다.")
            elif t.split()[2] == 's':
                # 숏 / 롱 포지션 정리
                order = binance_f.create_limit_sell_order(
                    symbol = t.split()[1],
                    amount = t.split()[4],
                    price= t.split()[3]
                )
                pprint.pprint(order)
                latest_order_id = order['info']['orderId']
                latest_order_symbol = order['symbol']
                context.bot.send_message(chat_id=update.effective_chat.id, text = "주문이 완료되었습니다.")
        except:
            context.bot.send_message(chat_id=update.effective_chat.id, text = 
            "Your order has not been received. Please check below\n"
            "1. Order sum must be greater than 10\n"
            "2. Make sure you have enough balance\n"
            "3. The transaction fee on Binance Exchange is 0.1%")


    def trade(self,update,context):
        t = update.message.text
        if t.split()[1] =='s': # 현물거래
            context.bot.send_message(chat_id=update.effective_chat.id, text = 
            "/order_s BTC/USDT b 50000 0.1: symbol buy/sell price amount")
            self.order_s(update,context)
        elif t.split()[1] =='f': # 선물거래
            context.bot.send_message(chat_id=update.effective_chat.id, text = 
            "/order_f BTC/USDT l 50000 0.1: symbol long/shot price amount\n"
            "Take the oppostie postion when you clear existing position")
            self.order_f(update,context)
    
    
    def cancel(self,update,context):
        ret = binance.cancel_order(
            id = self.__init__.latest_order_id,
            symbol = self.__init__.latest_order_symbol
        )
        print(ret) 
        context.bot.send_message(chat_id=update.effective_chat.id, text = "주문을 취소하였습니다.")
        

    def price(update,context): # 현재가 조회 
        t = update.message.text
        if t.split()[1] == 's':
            ticker = binance.fetch_ticker(t.split()[2])
            data = ticker['last']
            context.bot.send_message(chat_id=update.effective_chat.id, text = t.split()[2]+" 현재가 : "+str(data))
        elif t.split()[1] == 'f':
            ticker = binance_f.fetch_ticker(t.split()[2])
            data = ticker['last']
            context.bot.send_message(chat_id=update.effective_chat.id, text = t.split()[2]+" 현재가 : "+str(data))


    def warning(self,update,context,e): 
        # 봇이 문제가 생길 경우 가동 중단
        context.bot.send_message(chat_id=update.effective_chat.id, text = "에러 발생 : "+str(e))
        context.bot.send_message(chat_id=update.effective_chat.id, text = "\n봇 가동을 중단합니다.")
        self.updater.stop()


    def check(update,context):
        # 대기 주문 확인
        t = update.message.text
        open_orders = binance.fetch_open_orders(
            symbol = t.split()[1]
        )
        cnt = len(open_orders)
        context.bot.send_message(chat_id=update.effective_chat.id, text = "미체결된 주문은 "+cnt+"회 입니다.")
        for order in open_orders:
            context.bot.send_message(chat_id=update.effective_chat.id, text =str(order))



    # handler, dispatcher
    start_handler = CommandHandler('start',start)
    dispatcher.add_handler(start_handler) # start command를 요청하면 start function 실행

    stop_handler = CommandHandler('stop',stop)
    dispatcher.add_handler(stop_handler) 

    balance_handler = CommandHandler('balance',balance)
    dispatcher.add_handler(balance_handler) 

    market_handler = CommandHandler('market',market)
    dispatcher.add_handler(market_handler) 

    orders_handler = CommandHandler('order_s', order_s)
    dispatcher.add_handler(orders_handler) 

    orderf_handler = CommandHandler('order_f', order_f)
    dispatcher.add_handler(orderf_handler) 

    trade_handler = CommandHandler('trade', trade)
    dispatcher.add_handler(trade_handler) 

    cancel_handler = CommandHandler('cancel',cancel)
    dispatcher.add_handler(cancel_handler) 

    price_handler = CommandHandler('price',price)
    dispatcher.add_handler(price_handler) 

    warning_handler = CommandHandler('warning',warning) 
    dispatcher.add_handler(warning_handler) 

    try:
        updater.start_polling() # 코드가 종료되지 않고 계속 수행
    except Exception as e:
        warning(e)

#if __name__ =="__main__":
    #Tel = Telegram()




















