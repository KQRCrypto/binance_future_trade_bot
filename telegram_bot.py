import logging
import telegram

from telegram import Update
from telegram.ext import Updater,CommandHandler,CallbackContext


with open("../api.txt")as f:
    lines = f.readlines()
    api_key = lines[0].strip()
    secret = lines[1].strip()
    db = lines[2].strip()
    password = lines[3].strip()
    api_code = lines[4].strip()
    id = lines[5].strip()


bot = telegram.Bot(token=api_code)
updates = bot.getUpdates()
chat_id = bot.getUpdates()[-1].message.chat.id

for item in updates:
    print(item)

# updater, dispatcher 
updater = Updater(token=api_code, use_context=True)
dispatcher = updater.dispatcher


# /start 
def start(update,context):
    context.bot.send_message(chat_id=update.effective_chat.id, text = 
    "command\n"
    "/set 100 1 : 100초 간격 1% 변동성 알림\n"
    "/check : 현재 주기, 변동 퍼센트 확인\n"
    "/price 11000 : 11000$ 도달시 가격 알림\n"
    "/remove 11000 : 11000$ 알림 삭제\n"
    "/order btc b 50000 0.1 : symbol buy/sell 가격 수량\n"
    "/market btc b 0.1: symbol buy/sell 수량\n"
    "/ok : agree with placing order\n"
    "/cancel btc 123456 : cancel open order\n"
    "/pnl: show pnl\n"
    "/open btc : show all open orders\n")

# -> 우리 프로젝트에 맞춰서 command 수정 

"""
def set(update,context):
def check(update,context):
def price(update,context):
def remove(update,context):
def order(update,context):
def market(update,context):
def ok(update,context):
def cancel(update,context):
def pnl(update,context):
def open(update,context): 
"""


start_handler = CommandHandler('start',start)
set_handler = CommandHandler('set 100 1',set)
check_handler = CommandHandler('check',check)
price_handler = CommandHandler('price 11000',price)
remove_handler = CommandHandler('remove 11000',remove)
order_handler = CommandHandler('order btc b 50000 0.1',order)
market_handler = CommandHandler('market btc b 0.1',market)
ok_handler = CommandHandler('ok',ok)
cancel_handler = CommandHandler('cancel btc 123456',cancel)
pnl_handler = CommandHandler('pnl',pnl)
open_handler = CommandHandler('open btc',open)

dispatcher.add_handler(start_handler)
dispatcher.add_handler(set_handler)
dispatcher.add_handler(check_handler)
dispatcher.add_handler(price_handler)
dispatcher.add_handler(remove_handler)
dispatcher.add_handler(order_handler)
dispatcher.add_handler(market_handler)
dispatcher.add_handler(ok_handler)
dispatcher.add_handler(pnl_handler)
dispatcher.add_handler(open_handler)

updater.start_polling()


"""
# Enable logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO
)
logger = logging.getLogger(__name__)
"""




















