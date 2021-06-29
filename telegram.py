import telegram
api_key = ""
bot = telegram.Bot(token=api_key)

for item in bot.getUpdates():
    print(item)

    # id: -1001174905540