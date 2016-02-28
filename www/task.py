from pymongo import MongoClient
import time
import asyncio
import tushare as ts
import json

client = MongoClient()
db = client.mydb

async def do_main_task():
	day = time.strftime("%Y-%m-%d", time.localtime())
	print(day)
	day = "2016-02-26"
	# r = await asyncio.sleep(1)
	cursor = db.tasks.find({"date":day})
	status = 0
	for document in cursor:
		status = document["status"]
	# 获取指数
	if status < 1:
		print("task 1")
		df = ts.get_index()
		indexes = json.loads(df.to_json(orient='records'))
		for index in indexes:
			# print(index)
			index["date"] = day
			db.index_data.insert(index)
		status = 1
		db.tasks.update({"date":day}, {"date":day, "status":status}, upsert=True)	

	# 刷新股票列表，刷新股票交易数据
	if status < 2:
		print("task 2")
		df = ts.get_stock_basics()
		df.reset_index(level=0, inplace=True)
		df = df[['code','name','industry','area','pe','outstanding','totals','totalAssets','liquidAssets','fixedAssets','reserved','reservedPerShare','bvps','pb','timeToMarket']]
		stocks = json.loads(df.to_json(orient='records'))
		print("getting stock list")
		for stock in stocks:
			# print(stock)
			db.stocks.update_one({"code":stock["code"]}, {"$set":stock}, upsert=True)
		
		for stock in stocks:
			one_data = db.his_data.find_one({"code":stock["code"], "date":day})
			if one_data != None:
				continue
			
			try:
				df = ts.get_hist_data(stock["code"], start=day, end=day)
				h_data = json.loads(df.to_json(orient='records'))
				if len(h_data) > 0:
					print("get %s his data " % stock["code"])
					h_data = h_data[0]
					h_data["date"] = day
					h_data["code"] = stock["code"]
					db.his_data.update_one({"code":stock["code"], "date":day}, {"$set":h_data}, upsert=True)
			except Exception as e:
				print(e)

		status = 2
		db.tasks.update({"date":day}, {"date":day, "status":status}, upsert=True)
	
	if status < 3 :
		up_top = db.his_data.count({"date":day, "p_change":{"$gt":9.8}})
		down_top = db.his_data.count({"date":day, "p_change":{"$lt":-9.8}})
		up_num = db.his_data.count({"date":day, "p_change":{"$gte":0}})
		down_num = db.his_data.count({"date":day, "p_change":{"$lt":0}})

		db.day_stat.update_one({"date":day}, {"$set":{"date":day, "up_top":up_top, "down_top":down_top, "up_num":up_num, "down_num":down_num}}, upsert=True)
		status = 3
		db.tasks.update({"date":day}, {"date":day, "status":status}, upsert=True)

	if status < 4 :
		df = ts.sh_margins(start=day, end=day)
		sh_margin = json.loads(df.to_json(orient='records'))
		# print(sh_margin)
		sh_margin = sh_margin[0]
		db.day_stat.update_one({"date":day}, {"$set":{"sh_rzye":sh_margin["rzye"]}}, upsert=True)

		status = 4
		db.tasks.update({"date":day}, {"date":day, "status":status}, upsert=True)		
	print(status)

loop = asyncio.get_event_loop()
tasks = [do_main_task()]
loop.run_until_complete(asyncio.wait(tasks))
loop.close()	