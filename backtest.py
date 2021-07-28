import pandas as pd
import pymysql
import matplotlib.pyplot as plt
import openpyxl

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

#멀티팩터
#밴드폭을 활용한 스퀴즈 규제
#밴드 상/하단에 터치후 바로 튕겨나가지 않고 상/하단에 지속적으로 붙어 있다면 추세가 지속될 수 있다.
class backtest:
    # 바이낸스 선물 수수료 MAKER: 0.02%, TAKER: 0.04%
    fee_maker = 0.0002
    fee_taker = 0.0004
    def __init__(self, table, fig_num, leverage):
        self.table = table
        self.fig_num = fig_num
        self.leverage = leverage
        self.account = 10000  # 시작 잔고 10000$
        self.account_hist = [(self.account, 0, 'no_direction', 'start')]
        self.direction = 'long&short'
        self.id = 25  # 볼린저밴드가 MA25기준이므로 id=25인 지점부터 시작
        self.vdf = pd.DataFrame(columns=['id', 'mark', 'price'])
        self.fee = 0  #
        self.profit_count = 0  # 목표가 청산 횟수
        self.loss_count = 0  # 손절 횟수
        self.succesive_win = 0 #연승
        sql = '''SELECT *FROM `{0}`;'''.format(table)
        cursor.execute(sql)
        self.df = cursor.fetchall()
        self.df = pd.DataFrame(self.df)
        self.df = self.df.set_index('id')
        self.df['stddev'] = self.df['close'].rolling(window=25, min_periods=1).std()
        self.df['bandWidth'] = self.df['upperBB'] - self.df['lowerBB']
        self.df['bandWidthMean'] = self.df['bandWidth'].rolling(window=100, min_periods=1).mean()
        self.df['upper3std'] = self.df['MA25'] + self.df['stddev'] * 3
        self.df['lower3std'] = self.df['MA25'] - self.df['stddev'] * 3
    def profit_or_loss(self, mark, enter_price, stop_price, amount):
        for i in range(self.id, len(self.df)):
            high = self.df.iloc[i, 2]
            low = self.df.iloc[i, 3]
            MA25 = self.df.iloc[i, 8]
            upperBB = self.df.iloc[i, 10]
            lowerBB = self.df.iloc[i, 11]
            band75 = (upperBB + MA25) / 2
            band25 = (lowerBB + MA25) / 2
            if mark == 'long':
                if high >= band75:#목표가 청산
                    sell_price = band75
                    self.fee += sell_price*self.fee_maker * amount
                    self.id = i
                    pnl = (sell_price - enter_price) * amount - self.fee
                    self.account += pnl
                    self.account_hist.append((self.account, self.id, self.direction, 'long_목표가청산'))
                    # self.vdf.loc[self.id] = [self.id, 1, sell_price]
                    self.vdf = self.vdf.append(pd.DataFrame([[self.id, 1, sell_price]], columns=['id', 'mark','price']))
                    self.direction = 'long&short'
                    self.profit_count += 1
                    self.succesive_win += 1
                    break
                elif low <= stop_price:#손절
                    sell_price = stop_price
                    self.fee += sell_price * self.fee_taker * amount
                    self.id = i
                    pnl = (sell_price - enter_price) * amount - self.fee
                    self.account += pnl
                    self.account_hist.append((self.account, self.id, self.direction, 'long_손절'))
                    self.vdf = self.vdf.append(pd.DataFrame([[self.id, 2, sell_price]], columns=['id', 'mark','price']))
                    self.direction = 'short'
                    self.loss_count += 1
                    self.succesive_win = 0
                    break
            elif mark == 'short':
                if low <= band25:#목표가 청산
                    sell_price = band25
                    self.fee += sell_price * self.fee_maker * amount
                    self.id = i
                    pnl = (enter_price - sell_price) * amount - self.fee
                    self.account += pnl
                    self.account_hist.append((self.account, self.id, self.direction, 'short_목표가청산'))
                    self.vdf = self.vdf.append(pd.DataFrame([[self.id, 1, sell_price]], columns=['id', 'mark','price']))
                    self.direction = 'long&short'
                    self.profit_count +=1
                    self.succesive_win += 1
                    break
                elif high >= stop_price:#손절
                    sell_price = stop_price
                    self.fee += sell_price * self.fee_taker * amount
                    self.id = i
                    pnl = (enter_price - sell_price) * amount - self.fee
                    self.account += pnl
                    self.account_hist.append((self.account, self.id, self.direction, 'short_손절'))
                    self.vdf = self.vdf.append(pd.DataFrame([[self.id, 2, sell_price]], columns=['id', 'mark','price']))
                    self.direction = 'long'
                    self.loss_count +=1
                    self.succesive_win = 0
                    break
        return
    def bollinger_backtest(self, regulation):
        for enter in range(self.id, len(self.df)):
            if self.account<=100: #현실적으로 -99퍼 이상이면 청산
                print(len(self.vdf/2),"번 째 진입에서 청산")
                self.visualization()
                self.visual_perfomace()
                enter_count = int(len(self.vdf) / 2)
                return self.table, -100, -100, enter_count, self.profit_count/enter_count*100, self.loss_count/enter_count*100
            if enter < self.id:continue
            #규제조건에 포함되면 스킵
            if regulation == 1 and self.df.loc[enter].bandWidthMean >= self.df.loc[enter].bandWidth*2:
                self.id = enter
                continue
            self.fee = 0 #재 진입이므로 fee 초기화
            if self.direction == 'long&short':#양방, 직전 익절
                if self.df.loc[enter].low <= self.df.loc[enter].lowerBB:#밴드 하단터치 -> 밴드 하단 가격으로 롱 진입
                    enter_price = self.df.loc[enter].lowerBB
                    stop_price = self.df.loc[enter].lower3std
                    amount = self.account / enter_price * leverage
                    # if self.succesive_win>1: amount /= (self.succesive_win)
                    self.fee += enter_price*self.fee_maker * amount
                    self.id = enter
                    self.vdf = self.vdf.append(pd.DataFrame([[self.id, 0, enter_price]], columns=['id', 'mark','price']))
                    self.profit_or_loss('long', enter_price, stop_price, amount)
                elif self.df.loc[enter].high >= self.df.loc[enter].upperBB:
                    enter_price = self.df.loc[enter].upperBB
                    stop_price = self.df.loc[enter].upper3std
                    amount = self.account/enter_price * leverage
                    # if self.succesive_win > 1: amount /= (self.succesive_win)
                    self.fee += enter_price*self.fee_maker * amount
                    self.id = enter
                    self.vdf = self.vdf.append(pd.DataFrame([[self.id, 0, enter_price]], columns=['id', 'mark','price']))
                    self.profit_or_loss('short', enter_price, stop_price, amount)
            elif self.direction == 'long':#직전 손절
                if self.df.loc[enter].low <= self.df.loc[enter].lowerBB:#밴드 하단터치 -> 밴드 하단 가격으로 롱 진입
                    enter_price = self.df.loc[enter].lowerBB
                    stop_price = self.df.loc[enter].lower3std
                    amount = self.account/enter_price * leverage
                    self.fee += enter_price*self.fee_maker * amount
                    self.id = enter
                    self.vdf = self.vdf.append(pd.DataFrame([[self.id, 0, enter_price]], columns=['id', 'mark','price']))
                    self.profit_or_loss('long', enter_price, stop_price, amount)
            elif self.direction == 'short':#직전 손절
                if self.df.loc[enter].high >= self.df.loc[enter].upperBB:
                    enter_price = self.df.loc[enter].upperBB
                    stop_price = self.df.loc[enter].upper3std
                    amount = self.account/enter_price * leverage
                    self.fee += enter_price*self.fee_maker * amount
                    self.id = enter
                    self.vdf = self.vdf.append(pd.DataFrame([[self.id, 0, enter_price]], columns=['id', 'mark','price']))
                    self.profit_or_loss('short', enter_price, stop_price, amount)

        # self.visualization()
        ror = (self.account - 10000) / 100
        print(ror, len(self.vdf/2))
        mdd = self.visual_perfomace()
        enter_count = int(len(self.vdf) / 2)
        return self.table, ror, mdd, enter_count, self.profit_count/enter_count*100, self.loss_count/enter_count*100

    def visualization(self):
        plt.figure(self.fig_num,figsize=(10, 10))
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
                elif self.vdf.loc[i].mark == 1:  # 목표가 청산
                    # print(self.vdf.loc[i].price, i)
                    plt.plot(i, self.vdf.loc[i].price, 'rv')
                elif self.vdf.loc[i].mark == 2:  # 손절
                    plt.plot(i, self.vdf.loc[i].price, 'bv')
            except ValueError: # 동시간에 진입과 청산이 일어난 경우 valueError
                try:
                    if self.vdf[self.vdf.mark==0].loc[i].mark == 0:#진입
                        plt.plot(i, self.vdf[self.vdf.mark==0].loc[i].price, 'k^')
                    if self.vdf[self.vdf.mark==2].loc[i].mark ==2:#손절
                        # print(self.vdf[self.vdf.mark == 2].loc[i].mark)
                        plt.plot(i, self.vdf[self.vdf.mark==2].loc[i].price, 'bv')
                    if self.vdf[self.vdf.mark == 1].loc[i].mark == 1:  # 목표가 청산
                        # print(self.vdf[self.vdf.mark == 1].loc[i].price, i)
                        plt.plot(i, self.vdf[self.vdf.mark == 1].loc[i].price, 'rv')
                except KeyError:
                    pass
        plt.grid(True)
        plt.ylim([min(self.df.low), max(self.df.high)])
        plt.legend(loc='best')
        plt.title(self.table)
        plt.savefig('./backtest/charts/'+self.table+'.png')
        plt.close(self.fig_num)
        plt.close()
    def visual_perfomace(self):
        #ROR
        self.fig_num += 1
        plt.figure(self.fig_num,figsize=(10, 10))
        plt.subplot(311)
        # fig, ax1 = plt.subplots()
        plt.plot(range(len(self.account_hist)),[(i[0]-10000)/10000*100 for i in self.account_hist], label = 'ROR(%)', color='r')
        plt.ylabel("ROR(%)")
        plt.title(table+' '+str(self.leverage)+'X')
        plt.grid(True)
        plt.legend(loc='best')

        plt.subplot(312)
        plt.plot(range(len(self.account_hist)),[(i[0]-10000)/10000*100 for i in self.account_hist], label = 'log(ROR(%))', color='r')
        plt.yscale('symlog')
        plt.ylim([(min(self.account_hist)[0]-10000)/100*5, max(self.account_hist)[0]/100*5])
        plt.ylabel('log(ROR(%))')
        plt.grid(True)
        plt.legend(loc='best')
        #MDD
        plt.subplot(313)
        mdd_list = []
        local_high = 10000
        for account in self.account_hist:
            if account[0]> local_high: #신고가 갱신
                local_high = account[0]
            mdd = (local_high - account[0])/local_high*-100
            mdd_list.append(mdd)
        mdd = min(mdd_list)
        plt.plot(range(len(self.account_hist)),mdd_list, label = 'MDD')
        plt.xlabel("number_of_position_Entries")
        plt.ylabel("MDD(%)")
        plt.grid(True)
        plt.legend(loc='best')
        plt.savefig('./backtest/performances/'+table+'_performance_'+str(self.leverage)+'X.png')
        plt.close(self.fig_num-1)
        plt.close('all')
        return mdd

total_df = pd.DataFrame(columns=['table','수익률(%)', 'MDD(%)', '진입횟수','목표가청산(%)','손절(%)'])
total_df = total_df.set_index('table')
result_df = pd.DataFrame(columns=['table','수익률(%)', 'MDD(%)', '진입횟수','목표가청산(%)','손절(%)'])
target_table = ['btc_day', 'btc_4hour', 'btc_hour', 'btc_15minute',
                'eth_day', 'eth_4hour', 'eth_hour', 'eth_15minute',
                'bnb_day', 'bnb_4hour', 'bnb_hour', 'bnb_15minute',
                'xrp_day', 'xrp_4hour', 'xrp_hour', 'xrp_15minute',
                'ada_day', 'ada_4hour', 'ada_hour', 'ada_15minute']
# target_table = ['btc_day', 'btc_4hour', 'btc_hour']
leverage = 3
for i, table in enumerate(target_table):
    backtestObj = backtest(table, (i+1)*3, leverage)
    result_df.loc[table] = backtestObj.bollinger_backtest(0)#regulation: on = 1, off = 0
plt.show()
result_df = result_df.set_index('table')
result_df
pd.concat([total_df, result_df])
result_df.to_excel("result.xlsx")
