from pymongo import MongoClient

import tushare as ts
import json

client = MongoClient()
db = client.mydb

one_data = db.his_data.find_one({"code":"000000", "date":"2016-02-26"})

if one_data == None:
	print("None")


# df = ts.get_stock_basics()
# df.reset_index(level=0, inplace=True)
# df = df[['code','name']]
# #df = df.ix[df.code.str[0] == '3']
# df = df.sort('code').reset_index(drop=True)
# stocks = json.loads(df.to_json(orient='records'))
# print(stocks)
