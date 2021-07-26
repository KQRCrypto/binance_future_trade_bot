import pandas as pd
import pymysql
import matplotlib.pyplot as plt
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

#볼린저밴드 백테스트 의사코드
#1. 볼린저밴드 상단 터치시 롱 진입, 하단 터치시 숏 진입.
#2. 손절가 터치시 손절 후 반대 포지션으로만 진입
#3. 익절가 터치시 1번 대기
#4. 익절or손절 후 2번
class backtest:
    vdf = pd.DataFrame(columns=['id', 'mark','price'])
    amount = 10000  # 시작 잔고 10000$
    id = 25 # 볼린저밴드가 MA25기준이므로 id=25인 지점부터 시작
    direction = 'long&short'
    # 바이낸스 선물 수수료 MAKER: 0.02%, TAKER: 0.04%
    fee_maker = 0.0002
    fee_taker = 0.0004
    fee = 0
    def __init__(self, table):
        self.table = table
        sql = '''SELECT *FROM `{0}`;'''.format(table)
        cursor.execute(sql)
        self.df = cursor.fetchall()
        self.df = pd.DataFrame(self.df)
        self.df = self.df.set_index('id')
        self.df['stddev'] = self.df['close'].rolling(window=25, min_periods=1).std()
        self.df['upper3std'] = self.df['MA25'] + self.df['stddev'] * 3
        self.df['lower3std'] = self.df['MA25'] - self.df['stddev'] * 3
    def profit_or_loss(self, mark, enter_price, stop_price):
        for i in range(self.id, len(self.df)):
            high = self.df.iloc[i, 2]
            low = self.df.iloc[i, 3]
            MA25 = self.df.iloc[i, 8]
            upperBB = self.df.iloc[i, 10]
            lowerBB = self.df.iloc[i, 11]
            band75 = (upperBB + MA25) / 2
            band25 = (lowerBB + MA25) / 2
            if mark == 'long':
                if high >= band75:#익절
                    sell_price = band75
                    self.fee += sell_price*self.fee_maker
                    self.id = i
                    pnl = (sell_price - enter_price) * (self.amount/enter_price) - self.fee
                    self.amount += pnl
                    # self.vdf.loc[self.id] = [self.id, 1, sell_price]
                    self.vdf = self.vdf.append(pd.DataFrame([[self.id, 1, sell_price]], columns=['id', 'mark','price']))
                    self.direction = 'long&short'
                    break
                elif low <= stop_price:#손절
                    sell_price = stop_price
                    self.fee += sell_price * self.fee_taker
                    self.id = i
                    pnl = (sell_price - enter_price) * (self.amount/enter_price) - self.fee
                    self.amount += pnl
                    self.vdf = self.vdf.append(pd.DataFrame([[self.id, 2, sell_price]], columns=['id', 'mark','price']))
                    self.direction = 'short'
                    break
            elif mark == 'short':
                if low <= band25:#익절
                    sell_price = band25
                    self.fee += sell_price * self.fee_maker
                    self.id = i
                    pnl = (sell_price - enter_price) * (self.amount/enter_price) - self.fee
                    self.amount += pnl
                    self.vdf = self.vdf.append(pd.DataFrame([[self.id, 1, sell_price]], columns=['id', 'mark','price']))
                    self.direction = 'long&short'
                    break
                elif high >= stop_price:#손절
                    sell_price = stop_price
                    self.fee += sell_price * self.fee_taker
                    self.id = i
                    pnl = (sell_price - enter_price) * (self.amount/enter_price) - self.fee
                    self.amount += pnl
                    self.vdf = self.vdf.append(pd.DataFrame([[self.id, 2, sell_price]], columns=['id', 'mark','price']))
                    self.direction = 'long'
                    break
        return

    def bollinger_backtest(self):
        for enter in range(self.id, len(self.df)):
            if enter < self.id:continue
            self.fee = 0 #재 진입이므로 fee 초기화
            if self.direction == 'long&short':#양방
                if self.df.loc[enter].low <= self.df.loc[enter].lowerBB:#밴드 하단터치 -> 밴드 하단 가격으로 롱 진입
                    enter_price = self.df.loc[enter].lowerBB
                    stop_price = self.df.loc[enter].lower3std
                    self.fee += enter_price*self.fee_maker
                    self.id = enter
                    self.vdf = self.vdf.append(pd.DataFrame([[self.id, 0, enter_price]], columns=['id', 'mark','price']))
                    self.profit_or_loss('long', enter_price, stop_price)
                elif self.df.loc[enter].high >= self.df.loc[enter].upperBB:
                    enter_price = self.df.loc[enter].upperBB
                    stop_price = self.df.loc[enter].upper3std
                    self.fee += enter_price*self.fee_maker
                    self.id = enter
                    self.vdf = self.vdf.append(pd.DataFrame([[self.id, 0, enter_price]], columns=['id', 'mark','price']))
                    self.profit_or_loss('short', enter_price, stop_price)
            elif self.direction == 'long':
                if self.df.loc[enter].low <= self.df.loc[enter].lowerBB:#밴드 하단터치 -> 밴드 하단 가격으로 롱 진입
                    enter_price = self.df.loc[enter].lowerBB
                    stop_price = self.df.loc[enter].lower3std
                    self.fee += enter_price*self.fee_maker
                    self.id = enter
                    self.vdf = self.vdf.append(pd.DataFrame([[self.id, 0, enter_price]], columns=['id', 'mark','price']))
                    self.profit_or_loss('long', enter_price, stop_price)
            elif self.direction == 'short':
                if self.df.loc[enter].high >= self.df.loc[enter].upperBB:
                    enter_price = self.df.loc[enter].upperBB
                    stop_price = self.df.loc[enter].upper3std
                    self.fee += enter_price*self.fee_maker
                    self.id = enter
                    self.vdf = self.vdf.append(pd.DataFrame([[self.id, 0, enter_price]], columns=['id', 'mark','price']))
                    self.profit_or_loss('short', enter_price, stop_price)

        print(self.amount, len(self.vdf))
        self.visualization()
        return self.table, self.amount/10000*100, len(self.vdf)

    def visualization(self):
        plt.plot(self.df.index, self.df['close'], color='green', label='close')
        plt.plot(self.df.index, self.df['MA25'], 'y--', label='MA25')
        plt.plot(self.df.index, self.df.upperBB, 'y--', label='upper')
        plt.plot(self.df.index, self.df.lowerBB, 'y--', label='lower')
        plt.fill_between(self.df.index, self.df.upperBB, self.df.lowerBB, color='0.95')
        self.vdf = self.vdf.set_index('id')
        for i in self.vdf.index:
            try:
                if self.vdf.loc[i].mark == 0:  # 진입
                    plt.plot(i, self.vdf.loc[i].price, 'k^')
                elif self.vdf.loc[i].mark == 1:  # 익절
                    plt.plot(i, self.vdf.loc[i].price, 'rv')
                elif self.vdf.loc[i].mark == 2:  # 익절
                    plt.plot(i, self.vdf.loc[i].price, 'bv')
            except ValueError:
                try:
                    if self.vdf[self.vdf.mark==0].loc[i].mark == 0:#진입
                        plt.plot(i, self.vdf[self.vdf.mark==0].loc[i].price, 'k^')
                    if self.vdf[self.vdf.mark==2].loc[i].mark ==2:#손절
                        plt.plot(i, self.vdf[self.vdf.mark==2].loc[i].price, 'bv')
                    if self.vdf[self.vdf.mark == 1].loc[i].mark == 1:  # 익절
                        plt.plot(i, self.vdf[self.vdf.mark == 1].loc[i].price, 'rv')
                except KeyError:
                    # print('KeyError',i)
                    pass
        plt.grid(True)
        plt.ylim([min(self.df.low), max(self.df.high)])
        plt.legend(loc='best')
        plt.figure(figsize=(10, 10))
        plt.show()

if __name__ == '__main__':
    result_df = pd.DataFrame(columns=['table','수익률', '진입횟수'])
    target_table = ['btc_day', 'btc_4hour', 'btc_hour', 'btc_15minute']
    # target_table = ['btc_day']
    for table in target_table:
        backtestObj = backtest(table)
        result_df.loc[table] = backtestObj.bollinger_backtest()
    result_df
