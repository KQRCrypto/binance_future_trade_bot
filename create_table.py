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

table_list = ['eth_15minute','eth_hour','eth_4hour','eth_day']
table_list = ['_15minute', '_hour', '_4hour', '_day']
table_list = ['_15minute', '_hour', '_4hour', '_day']
table_list = ['bnb_15minute', 'bnb_hour', 'bnb_4hour', 'bnb_day']
table_list = ['xrp_15minute', 'xrp_hour', 'xrp_4hour', 'xrp_day']
table_list = ['ada_15minute', 'ada_hour', 'ada_4hour', 'ada_day']
for table in table_list:
    sql = '''CREATE TABLE IF NOT EXISTS `{0}` LIKE `btc_day`;'''.format(table)
    cursor.execute(sql)
    db.commit()