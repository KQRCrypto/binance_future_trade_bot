import ccxt
import pymysql

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

#선물거래용 바이낸스 객체 생성
binanceObj = ccxt.binance(config={
    'apiKey': api_key,
    'secret': secret,
    'enableRateLimit' : True,
    'options':{
        'defaultType':'future'
    }
})
def timestamp_to_str(time): # 타임스탬프 문자열로 변환
    return str(time.year*100000000 + time.month*1000000 + time.day*10000 + time.hour*100 + time.minute)
tf_table = [['1m', 'realtime_btc_minute'], ['15m', 'realtime_btc_15minute'],
             ['1h', 'realtime_btc_hour'], ['4h', 'realtime_btc_4hour'], ['1d', 'realtime_btc_day']]#1열은 timeframe, 2열은 테이블 명

binanceObj.fetch_ohlcv("BTC/USDT", timeframe='1d', limit=100)

for tf in tf_table:#realtime 전체 테이블 삭제
    sql = '''TRUNCATE `{0}`'''.format(tf[1])
    cursor.execute(sql)
    db.commit()