import config
import json
import pytz
import sys
from StringIO import StringIO
from urllib import urlopen
from zipfile import ZipFile
from csv import DictReader
from datetime import datetime, time, timedelta
from HRTDatabase import HRTDatabase

eastern = pytz.timezone('US/Eastern')

def openDatabase():
	c = config.load()
	return HRTDatabase(c["db_uri"], c["db_name"])

def downloadGtfsData():
	feedUrl = "http://www.gtfs-data-exchange.com/api/agency?agency=hampton-roads-transit-hrt"
	fileUrl = json.loads(urlopen(feedUrl).read())['data']['datafiles'][0]['file_url']
	return ZipFile(StringIO(urlopen(fileUrl).read()))

def estNow():
	return datetime.utcnow().replace(tzinfo=pytz.utc).astimezone(eastern)

def weekdayStr(date):
	days = ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday']
	return days[date.weekday()]

def runForDate(curDate):
	daysFromNow = 1
	if len(sys.argv) == 2:
		daysFromNow = int(sys.argv[1])
	runForDate = (curDate + timedelta(days=daysFromNow)).date()
	return datetime.combine(runForDate, time.min)

def removeOldData(date):
	removeOlderThanDate = eastern.localize(datetime.combine(date.date(), time.min), is_dst=None)
	print 'Removing GTFS data older than ' + str(removeOlderThanDate)
	db.removeOldGTFS(removeOlderThanDate.astimezone(pytz.utc))

def getGtfsActiveServices(zipFile, date, weekday):
	activeServiceIds = []
	calendar = DictReader(zipFile.open("calendar.txt"))
	for row in calendar:
		start = datetime.strptime(row['start_date'], "%Y%m%d").date()
		end = datetime.strptime(row['end_date'], "%Y%m%d").date()
		if date.date() >= start and date.date() <= end and row[weekday] == '1':
			activeServiceIds.append(row['service_id'])
	return activeServiceIds

def getGtfsActiveTrips(zipFile, serviceIds):
	activeTrips = {}
	trips = DictReader(zipFile.open("trips.txt"))
	for row in trips:
		if row['service_id'] in serviceIds:
			activeTrips[row['trip_id']] = row
	return activeTrips

def getGtfsActiveStopTimes(zipFile, trips, date):
	activeStopTimes = []
	stopTimes = DictReader(zipFile.open("stop_times.txt"))
	for row in stopTimes:
		if row['trip_id'] in trips:
			try:
				trip = trips[row['trip_id']]
				row['route_id'] = int(trip['route_id'])
				row['direction_id'] = int(trip['direction_id'])
				row['block_id'] = trip['block_id']
				row['stop_id'] = int(row['stop_id'])
				row['stop_sequence'] = int(row['stop_sequence'])
				
				arriveTime = row['arrival_time'].split(':')
				naiveArriveTime = date + timedelta(hours=int(arriveTime[0]), minutes=int(arriveTime[1]))
				localArriveTime = eastern.localize(naiveArriveTime, is_dst=None)
				row['arrival_time'] = localArriveTime.astimezone(pytz.utc)
				
				departTime = row['departure_time'].split(':')
				naiveDeptTime = date + timedelta(hours=int(departTime[0]), minutes=int(departTime[1]))
				localDeptTime = eastern.localize(naiveDeptTime, is_dst=None)
				row['departure_time'] = localDeptTime.astimezone(pytz.utc)
				
				activeStopTimes.append(row)
			except ValueError:
				pass
	return activeStopTimes

def getGtfsStops(zipFile):
	stops = []
	stopsReader = DictReader(zipFile.open("stops.txt"))
	for row in stopsReader:
		try:
			stops.append({
				'stopId': int(row['stop_id']),
				'stopName': row['stop_name'],
				'location': [float(row['stop_lon']), float(row['stop_lat'])]
			})
		except ValueError:
			pass
	return stops

def getGtfsRoutes(zipFile):
	routes = []
	routesReader = DictReader(zipFile.open("routes.txt"))
	for row in routesReader:
		try:
			row['route_id'] = int(row['route_id'])
			routes.append(row)
		except ValueError:
			pass
	return routes

now = estNow()
print 'Running at ' + str(now)

db = openDatabase()
removeOldData(now)

zipFile = downloadGtfsData()
runDate = runForDate(now)
runWeekday = weekdayStr(runDate)

print 'Running for ' + runWeekday + ', ' + str(runDate)

activeServiceIds = getGtfsActiveServices(zipFile, runDate, runWeekday)
print 'Active Service Ids: ' + str(activeServiceIds)

activeTrips = getGtfsActiveTrips(zipFile, activeServiceIds)
print str(len(activeTrips)) + ' Active Trips'

activeStopTimes = getGtfsActiveStopTimes(zipFile, activeTrips, runDate)
print str(len(activeStopTimes)) + ' Active Stop Times'

db.insertGTFS(activeStopTimes, runDate)

stops = getGtfsStops(zipFile)
print str(len(stops)) + " stops"
db.insertStops(stops)

routes = getGtfsRoutes(zipFile)
print str(len(routes)) + " routes"
db.insertRoutes(routes)