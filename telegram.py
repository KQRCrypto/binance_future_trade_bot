import telegram
api_key = 1867497311:AAHrvdQv-k2s7RoMmJNkJo875LSxLMnGjog
bot = telegram.Bot(token=api_key)

for item in bot.getUpdates():
    print(item)

    # id: -1001174905540