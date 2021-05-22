### Report Rebalance& Grid  !!!!! ####

# import neccessary package

import ccxt
import json
import numpy as np
import pandas as pd
import time
import decimal
from datetime import datetime
import pytz
import csv
import sys

# Api and secret
api_key     = ""  
api_secret  = ""
subaccount  = ""
# Set your account name (ตั้งชื่อ Account ที่ต้องการให้แสดงผลในไฟล์ report)
account_name = "Report_Rebalance"  



# Exchange Details
exchange = ccxt.ftx({
    'apiKey': api_key,
    'secret': api_secret,
    'enableRateLimit': True}
)
exchange.headers = {'FTX-SUBACCOUNT': subaccount,}
post_only = True  # Maker or Taker (วางโพซิชั่นเป็น MAKER เท่านั้นหรือไม่ True = ใช่)

# Global Varibale Setting
token_name_lst  = ["SOL"]       # --- change ----# Name of Token (ใส่ชื่อเหรียญที่ต้องการ)
pair_lst        = ["SOL/USD"]   # --- change ----# Pair (ใส่ชื่อคู่ที่ต้องการ Rebalance เช่น XRP จะเป็น ["XRP/USD"])
pair            = "SOL/USD"     # --- change ----# Pair (ใส่ชื่อคู่ที่ต้องการ Rebalance เช่น XRP จะเป็น "XRP/USD")

pair_dict       = {token_name_lst[i]: pair_lst[i] for i in range(len(token_name_lst))}

# file system
tradelog_file = "{}_TradingLog.csv".format(account_name)
trading_call_back = 1000        # --- change ----# จำนวนการดึง transaction ออกมาย้อนหลัง


def get_time():  
    named_tuple = time.localtime() # get struct_time
    Time = time.strftime("%m/%d/%Y, %H:%M:%S", named_tuple)
    return Time


def get_wallet_details():
    wallet = exchange.privateGetWalletBalances()['result']
    return wallet


def get_minimum_size():
    minimum_size = float(exchange.fetch_ticker(pair)['info']['minProvideSize'])
    return minimum_size


def checkDB():
    try:
        tradinglog = pd.read_csv("{}_tradinglog.csv".format(account_name))
        print('DataBase Exist Loading DataBase....')
    except:
        tradinglog = pd.DataFrame(columns=['id', 'timestamp', 'date','time', 'pair', 'side', 'price', 'qty', 'cost', 'fee', 'liquidity', 'bot_name', 'subaccount'])
        tradinglog.to_csv("{}_tradinglog.csv".format(account_name),index=False)
        print("Database Created")
        
    return tradinglog


def get_trade_history(pair):
    pair = pair
    trade_history = pd.DataFrame(exchange.fetchMyTrades(pair, limit = trading_call_back),
                              columns=['id', 'timestamp', 'timestamp','datetime', 'symbol', 'side', 'price', 'amount', 'cost', 'fee', 'takerOrMaker'])
    cost=[]
    for i in range(len(trade_history)):
        fee = trade_history['fee'].iloc[i]['cost'] if trade_history['fee'].iloc[i]['currency'] == 'USD' else trade_history['fee'].iloc[i]['cost'] * trade_history['price'].iloc[i]
        cost.append(fee)  # ใน fee เอาแค่ cost
    
    trade_history['fee'] = cost
    
    return trade_history


def get_last_id(pair):
    pair = pair
    trade_history = get_trade_history(pair)
    last_trade_id = (trade_history.iloc[:trading_call_back]['id'])
    
    return last_trade_id


def update_trade_log():
    checkDB()

    tradinglog = pd.read_csv("{}_tradinglog.csv".format(account_name))
    last_trade_id = get_last_id(pair)
    trade_history = get_trade_history(pair)
    
    for i in last_trade_id:
        tradinglog = pd.read_csv("{}_tradinglog.csv".format(account_name))
        trade_history = get_trade_history(pair)
        #print(trade_history)
        if int(i) not in tradinglog.values:
            print(i not in tradinglog.values)
            last_trade = trade_history.loc[trade_history['id'] == i]
            list_last_trade = last_trade.values.tolist()[0]

            # แปลงวันที่ใน record
            d = datetime.strptime(list_last_trade[3], "%Y-%m-%dT%H:%M:%S.%fZ")
            d = pytz.timezone('Etc/GMT+7').localize(d)
            d = d.astimezone(pytz.utc)
            Date = d.strftime("%Y-%m-%d")
            Time = d.strftime("%H:%M:%S")
            
            # edit & append ข้อมูลก่อน add เข้า database
            list_last_trade[2] = Date
            list_last_trade[3] = Time
            list_last_trade.append(account_name)
            list_last_trade.append(subaccount)

            with open("{}_tradinglog.csv".format(account_name), "a+", newline='') as fp:
                wr = csv.writer(fp, dialect='excel')
                wr.writerow(list_last_trade)
            print('Recording Trade ID : {}'.format(i))

        else:
            print('Trade Already record')
    print("Calculating.")

# Create Buy and Sell dataframe separately
def Buy_Sell_Dataframe(trade_history , pair):
    print("Calculating...")

    min_trade_size      = get_minimum_size()

    trade_history_buy   = pd.DataFrame(trade_history)
    trade_history_sell  = pd.DataFrame(trade_history)

    trade_history_buy   = trade_history[(trade_history['side']=='buy') & (trade_history['liquidity']=='maker') & (trade_history['pair']==pair)]
    trade_history_sell   = trade_history[(trade_history['side']=='sell') & (trade_history['liquidity']=='maker') & (trade_history['pair']==pair)]
    
    for i in range(len(trade_history_buy)):
        amount  = trade_history_buy['qty'].iloc[i]
        amount  = amount.tolist()
        times   = (round(amount / min_trade_size))

        series  = pd.Series(trade_history_buy.iloc[i])
        series['qty']   = min_trade_size
        series['fee']   = series['fee'] / times
        
        for number in range(times):    
            trade_history_buy = trade_history_buy.append(series)

    for i in range(len(trade_history_sell)):
        amount  = trade_history_sell['qty'].iloc[i]
        amount  = amount.tolist()
        times   = (round(amount / min_trade_size))

        series  = pd.Series(trade_history_sell.iloc[i])
        series['qty']   = min_trade_size
        series['fee']   = series['fee'] / times
        
        for number in range(times):    
            trade_history_sell = trade_history_sell.append(series)

    trade_history_buy.sort_values(by=['timestamp'], inplace=True, ascending=False)
    trade_history_sell.sort_values(by=['timestamp'], inplace=True, ascending=False)

    trade_history_buy = trade_history_buy[trade_history_buy.qty == min_trade_size]
    trade_history_sell = trade_history_sell[trade_history_sell.qty == min_trade_size]

    trade_history_buy.reset_index(drop=True, inplace=True)
    trade_history_sell.reset_index(drop=True, inplace=True)

    column_list_1 = ['id1', 'timestamp1', 'date1','time1', 'pair1', 'side1', 'price1', 'qty1', 'cost1', 'fee1', 'liquidity1', 'bot_name1', 'subaccount1']
    column_list_2 = ['id2', 'timestamp2', 'date2', 'time2', 'pair2', 'side2', 'price2', 'qty2', 'cost2', 'fee2', 'liquidity2', 'bot_name2', 'subaccount2']
    
    trade_history_buy.columns = column_list_1
    trade_history_sell.columns = column_list_2

    return trade_history_buy, trade_history_sell
    
# Match order and pop-out the remainer
def Matching(df_1, df_2):
    print("Calculating....")

    if len(df_1) >= len(df_2):
        index_number    = len(df_2)
        trade_history   = pd.concat([df_1.iloc[:index_number], df_2], axis=1)
        remain_df       = df_1.iloc[index_number:]

    else:
        index_number    = len(df_1)
        trade_history   = pd.concat([df_1, df_2.iloc[:index_number]], axis=1)
        remain_df       = df_2.iloc[index_number:]

    trade_history.reset_index(drop=True, inplace=True)
    remain_df.reset_index(drop=True, inplace=True)

    return trade_history, remain_df

# Return Remain Data that will use in nect week
def Remain_Data(trade_history):
    df          = trade_history.copy()
    #df.columns  = df.columns.droplevel()

    column_list = ['id', 'timestamp', 'date', 'time', 'pair', 'side', 'price', 'qty', 'cost', 'fee', 'liquidity', 'bot_name', 'subaccount']

    df.columns  = column_list

    return df


# Include profit and loss
def Profit_Loss(trade_history, pair):

    buy_amount      = np.dot(trade_history['qty1'], trade_history['price1'])
    sell_amount     = np.dot(trade_history['qty2'], trade_history['price2'])
    total_fee       = sum(trade_history['fee1']) + sum(trade_history['fee2'])

    profit_loss     = sell_amount - buy_amount
    net_profit_loss = profit_loss - total_fee

    total_transaction = len(trade_history) * 2

    return profit_loss, total_fee, net_profit_loss, total_transaction


# Report asset returns
def Return_of_Asset(pair, pnl, fee, net_pnl, transaction):

    report_value    = [['Profit (Loss)', pnl],
                        ['Total Fee', fee],
                        ['Net Profit (Loss)', net_pnl],
                        ['Total Transaction', transaction]]

    column_list     = [["Report", "value"]]
    column_list     = pd.MultiIndex.from_product(column_list)
    return_report   = pd.DataFrame(report_value, columns=column_list)

    return return_report


# Return 3 product files; matching, remaining, report
def File_Product(pair):

    match_df            = pd.DataFrame()
    remain_df           = pd.DataFrame()
    remain_df_report    = pd.DataFrame()

    profit_loss         = 0
    total_fee           = 0
    net_profit_loss     = 0
    total_transaction   = 0

    # loop Buy_Sell_Dataframe มีปัญหา
    buy, sell           = Buy_Sell_Dataframe(trade_history, pair)
    match_order, remain_order       = Matching(buy, sell)
    pnl, fee, net_pnl, transaction  = Profit_Loss(match_order, pair)

    profit_loss         += pnl
    total_fee           += fee
    net_profit_loss     += net_pnl
    total_transaction   += transaction

    return_on_asset     = Return_of_Asset(pair, pnl, fee, net_pnl, transaction)
    match_df            = pd.concat([match_df, match_order], join='outer', axis=1)
    match_df            = pd.concat([match_df, return_on_asset], join='outer', axis=1)
    remain_df_report    = pd.concat([remain_df_report, remain_order], join='outer', axis=1)

    # remain data that will use in next time
    remain_recolumn     = Remain_Data(remain_order)
    remain_df           = pd.concat([remain_df, remain_recolumn], axis=0)

    return_on_port  = Return_of_Asset('Portfolio', profit_loss, total_fee, net_profit_loss, total_transaction)
    match_df        = pd.concat([match_df, return_on_port], join='outer', axis=1)

    remain_df.reset_index(drop=True, inplace=True)

    return match_df, remain_df, remain_df_report

if __name__ == "__main__":
    try:    
        wallet = get_wallet_details()
    
        for item in wallet:
            asset_name = item['coin']
        
            if asset_name != 'USD':
                
                pair = pair_dict[asset_name]
                print(pair)
                update_trade_log()
                
                trade_history = pd.read_csv("{}_tradinglog.csv".format(account_name))   

        print("Calculating..")
        match_file, remain_file, remain_report = File_Product(pair)

        export_file = True
        if export_file:
            match_file.to_csv("{}_match_order.csv".format(account_name))        
            remain_file.to_csv("{}_remain_order.csv".format(account_name))      
            remain_report.to_csv("{}_remain_report.csv".format(account_name))   
            print('Export files Success')
        else:
            print("Don't export any files")

    except KeyboardInterrupt:
        sys.exit()
        print("Exit")

    except Exception as e:
        print('Error : {}'.format(str(e)))
        time.sleep(60) 



