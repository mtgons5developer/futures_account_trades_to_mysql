import mysql.connector
from mysql.connector import errorcode
import datetime
import pandas as pd

from binance import Client
import config

client = Client(config.BINANCE_API_KEY,config.BINANCE_SECRET_KEY)

def get_df(start_date, end_date=None):
    start_date_timestamp = int(datetime.datetime.strptime(str(start_date), '%Y-%m-%d').timestamp()*1000)
    if end_date:
        end_date_timestamp = int(datetime.datetime.strptime(str(end_date), '%Y-%m-%d').timestamp()*1000)
        df = pd.DataFrame(client.futures_account_trades(startTime=start_date_timestamp, endTime=end_date_timestamp, recvWindow=6000000))
    else: df = pd.DataFrame(client.futures_account_trades(startTime=start_date_timestamp, recvWindow=6000000))
    return df

try:
    
    cnx = mysql.connector.connect(user=config.USER, password=config.PASSWORD, host=config.HOST, database=config.DATABASE)                               

except mysql.connector.Error as err:
    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        print("Something is wrong with your user name or password")
    elif err.errno == errorcode.ER_BAD_DB_ERROR:
        print("Database does not exist")
    else:
        print(err)
else:

    date = "2022-03-07"

    reader = get_df(date)
    df = pd.DataFrame(reader)

    df["id"] = df["id"]
    df["orderId"] = df["orderId"]
    df["time"] = df["time"]
    
    df['symbol'] = reader['symbol']
    df['marginAsset'] = reader['marginAsset']
    df['commissionAsset'] = reader['commissionAsset']
    df['positionSide'] = reader['positionSide']
    df['buyer'] = reader['buyer']
    df['maker'] = reader['maker']
    df["side"] = df["side"]

    df["price"] = df["price"]
    df["qty"] = df["qty"]
    df["quoteQty"] = df["quoteQty"]
    df["commission"] = df["commission"]
    df["realizedPnl"] = df["realizedPnl"]

    num = len(df.loc[df['id'].idxmin():df['id'].idxmax()])
    
    cursor = cnx.cursor()
    print("Connected")        
    
    query = """INSERT IGNORE INTO Trade_History (symbol, id, orderId, side, price, qty, realizedPnl, marginAsset, time, quoteQty, commission, commissionAsset, positionSide, buyer, maker) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

    for x in range(num):
        symbol = df['symbol'][x]        
        orderId = df['orderId'][x]
        id = df['id'][x]
        side = df['side'][x]
        price = df['price'][x]
        qty = df['qty'][x]
        realizedPnl = df['realizedPnl'][x]
        marginAsset = df['marginAsset'][x]
        quoteQty = df['quoteQty'][x]
        commission = df['commission'][x]
        commissionAsset = df['commissionAsset'][x]        
        timee = df['time'][x]
        positionSide = df['positionSide'][x]
        buyer = df['buyer'][x]
        maker = df['maker'][x]        

        values = (symbol, int(id), int(orderId), side, float(price), float(qty), float(realizedPnl), marginAsset, int(timee), float(quoteQty), float(commission), commissionAsset, positionSide, bool(buyer), bool(maker))

        cursor.execute(query, values)
        
    cnx.commit()
    cursor.close()
    cnx.close()
    print("Completed")

