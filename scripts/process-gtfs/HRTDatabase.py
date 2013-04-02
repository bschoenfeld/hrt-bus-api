from datetime import datetime, timedelta
from pymongo import Connection, GEO2D, ASCENDING

class HRTDatabase:
	def __init__(self, uri, dbName):
		self.client = Connection(uri)
		self.db = self.client[dbName]
	
	def insertGTFS(self, data, date):
		self.db['gtfs'].remove({"arrival_time": {"$gte": date, "$lt": date + timedelta(days=1)}})
		self.db['gtfs'].insert(data)
	
	def removeOldGTFS(self, date):
		self.db['gtfs'].remove({"arrival_time": {"$lt": date}})
	
	def insertStops(self, data):
		self.db['stops'].remove()
		self.db['stops'].insert(data)
		self.db['stops'].ensure_index( [('location', GEO2D)] )
	
	def insertRoutes(self, data):
		self.db['routes'].remove()
		self.db['routes'].insert(data)
	
