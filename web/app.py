from datetime import datetime, timedelta
from flask import Flask, Response, render_template, redirect, url_for, request, current_app
from functools import wraps
from geopy import geocoders
from google.protobuf import text_format
import json
import os
import pymongo
from bson import json_util
import gtfs_realtime_pb2


app = Flask(__name__)
dthandler = lambda obj: obj.isoformat() if isinstance(obj, datetime) else None


db = None
curDateTime = None
collectionPrefix = None


def support_jsonp(f):
    """Wraps JSONified output for JSONP"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        callback = request.args.get('callback', False)
        if callback:
            content = str(callback) + '(' + str(f(*args, **kwargs)) + ')'
            return current_app.response_class(content, mimetype='application/json')
        else:
            return Response(f(*args, **kwargs), mimetype='application/json')
    return decorated_function


@app.before_request
def beforeRequest():
    global db
    global curDateTime
    global collectionPrefix

    db = pymongo.MongoClient(os.environ['MONGODB_URI']).get_database(None)
    curDateTime = datetime.utcnow() + timedelta(hours=-5)
    collectionPrefix = curDateTime.strftime('%Y%m%d')


@app.route('/')
def index():
    return redirect(url_for('getApiInfo'))


@app.route('/gtfs/trip_update/')
def tripUpdate():
    # PROTOCAL BUFFER!!!
    # https://developers.google.com/protocol-buffers/docs/pythontutorial

    # Create feed
    feed = gtfs_realtime_pb2.FeedMessage()

    # header
    feed.header.gtfs_realtime_version = '1.0'
    feed.header.timestamp = long(
        (datetime.utcnow() - datetime(1970, 1, 1)).total_seconds())

    # create an entity for each active trip id
    activeTrips = db['checkins'].aggregate([{"$match":
                                             {"tripId": {'$ne': None},
                                                 "adherence": {'$exists': True},
                                                 "lastStopSequence": {'$exists': True},
                                              "lastStopSequenceOBA": {'$exists': True}}},
                                            {"$sort": {"time": 1}},
                                            {"$group":
                                                {"_id": {"trip": "$tripId", "seq": "$lastStopSequence"},
                                                 "time": {"$last": "$time"},
                                                 "bus": {"$last": "$busId"},
                                                 "adherence": {"$last": "$adherence"},
                                                 "seqOBA": {"$last": "$lastStopSequenceOBA"}}},
                                            {"$sort": {"_id.seq": 1}},
                                            {"$group":
                                                {"_id": {"trip": "$_id.trip"},
                                                 "time": {"$last": "$time"},
                                                 "bus": {"$last": "$bus"},
                                                 "timeChecks":
                                                    {"$push":
                                                        {"seq": "$_id.seq",
                                                         "time": "$time",
                                                         "adherence":  "$adherence",
                                                         "seqOBA":  "$seqOBA"}}}}])
    # return json.dumps(activeTrips, default=dthandler)

    for trip in activeTrips['result']:
        # add the trip entity
        entity = feed.entity.add()
        entity.id = 'trip' + trip['_id']['trip']
        entity.trip_update.trip.trip_id = trip['_id']['trip']
        entity.trip_update.vehicle.id = str(trip['bus'])
        entity.trip_update.vehicle.label = str(trip['bus'])
        entity.trip_update.timestamp = long(
            (trip['time'] - datetime(1970, 1, 1)).total_seconds())

        # add the stop time updates
        for update in trip['timeChecks']:
            stopTime = entity.trip_update.stop_time_update.add()
            stopTime.stop_sequence = update['seq'] if not request.args.get('oba') else update[
                'seqOBA']
            stopTime.arrival.delay = update[
                'adherence'] * -60  # convert minutes to seconds

    if request.args.get('debug'):
        return text_format.MessageToString(feed)
    return feed.SerializeToString()


@app.route('/gtfs/vehicle_position/')
def vehiclePosition():
    # PROTOCAL BUFFER!!!
    # https://developers.google.com/protocol-buffers/docs/pythontutorial

    # Create feed
    feed = gtfs_realtime_pb2.FeedMessage()

    # header
    feed.header.gtfs_realtime_version = '1.0'
    feed.header.timestamp = long(
        (datetime.utcnow() - datetime(1970, 1, 1)).total_seconds())

    # create an entity for each active trip id
    lastBusLocations = db['checkins'].aggregate([{"$match":
                                                  {"tripId": {'$ne': None},
                                                   "location": {'$exists': True}}},
                                                 {"$group":
                                                  {"_id": {"bus": "$busId"},
                                                   "trip": {"$last": "$tripId"},
                                                   "time": {"$last": "$time"},
                                                   "location": {"$last": "$location"}}}])
    # return json.dumps(lastLocations, default=dthandler)

    for bus in lastBusLocations['result']:
        # add the trip entity
        entity = feed.entity.add()
        entity.id = 'bus' + str(bus['_id']['bus'])
        entity.vehicle.trip.trip_id = bus['trip']
        entity.vehicle.vehicle.id = str(bus['_id']['bus'])
        entity.vehicle.vehicle.label = str(bus['_id']['bus'])
        entity.vehicle.position.latitude = float(bus['location'][0])
        entity.vehicle.position.longitude = float(bus['location'][1])
        entity.vehicle.timestamp = long(
            (bus['time'] - datetime(1970, 1, 1)).total_seconds())

    if request.args.get('debug'):
        return text_format.MessageToString(feed)
    return feed.SerializeToString()


@app.route('/api/')
@support_jsonp
def getApiInfo():
    return json.dumps({'version': '0.10', 'dbHost': db.client.address, 'curDateTime': curDateTime, 'collectionPrefix': collectionPrefix}, default=dthandler)


@app.route('/api/routes/active/')
@support_jsonp
def getActiveRoutes():
    # List the routes from the checkins
    activeRoutes = db['checkins'].find(
        {'location': {'$exists': True}}).distinct('routeShortName')

    # Get details about those routes from the GTFS data
    activeRoutesWithDetails = db['routes_' + collectionPrefix].find(
        {'route_short_name': {'$in': activeRoutes}}, {'_id': 0}).sort('route_short_name')
    return json.dumps(list(activeRoutesWithDetails))


@app.route('/api/buses/on_route/<int:routeShortName>/')
@support_jsonp
def getBusesOnRoute(routeShortName):
    # Get all checkins for the route, only keep the last one for each bus
    checkins = {}
    for checkin in db['checkins'].find({'routeShortName': routeShortName, 'location': {'$exists': True}}, {'_id': 0}).sort('time'):
        checkins[checkin['busId']] = checkin
    return json.dumps(checkins.values(), default=dthandler)


@app.route('/api/buses/routes')
@app.route('/api/buses/routes/<path:routeShortNames>/')
@support_jsonp
def getBusesByRoute(routeShortNames=None):
    match = {'location': {'$exists': True}}
    if routeShortNames is not None:
        ids = map(int, filter(None, routeShortNames.split('/')))
        match['routeShortName'] = {'$in': ids}

    cursor = db['checkins'].find(match).sort('time')
    checkins = {}
    for c in cursor:
        c['_id'] = str(c['_id'])
        checkins[c['busId']] = c

    return json.dumps(checkins.values(), default=dthandler)


@app.route('/api/buses/history/<int:busId>/')
@support_jsonp
def getBusHistory(busId):
    # Get all checkins for a bus
    checkins = db['checkins'].find({'busId': busId, 'location': {'$exists': True}}, {
                                   '_id': 0, 'tripId': 0}).sort('time', pymongo.DESCENDING)
    return json.dumps(list(checkins), default=dthandler)


@app.route('/api/stops/near/intersection/<city>/<intersection>/')
def getStopsNearIntersection(city, intersection):
    place, (lat, lng) = geocoders.googlev3.GoogleV3().geocode(
        "{0}, {1}, VA".format(intersection, city), exactly_one=False)[0]
    return getStopsNear(lat, lng)


@app.route('/api/stops/near/<lat>/<lng>/')
@support_jsonp
def getStopsNear(lat, lng):
    stops = db['stops_' + collectionPrefix].find(
        {"location": {"$near": [float(lat), float(lng)]}}).limit(6)
    stops = list(stops)
    for stop in stops:
        stop['_id'] = str(stop['_id'])
    return json.dumps(stops)


@app.route('/api/stops/id/<path:stopIds>/')
@support_jsonp
def getStopsById(stopIds):
    ids = stopIds.split('/')
    stops = db['stops_' + collectionPrefix].find({'stopId': {'$in': ids}})
    stops = list(stops)
    for stop in stops:
        stop['_id'] = str(stop['_id'])
    return json.dumps(stops)


@app.route('/api/stop_times/<int:routeShortName>/<stopId>/')
@support_jsonp
def getNextBus(routeShortName, stopId):
    scheduledStops = db['gtfs_' + collectionPrefix].find({'route_short_name': routeShortName, 'stop_id': stopId, 'arrival_time': {
                                                         '$gte': datetime.utcnow()}}).sort('arrival_time').limit(3)
    lastStop = db['gtfs_' + collectionPrefix].find({'route_short_name': routeShortName, 'stop_id': stopId, 'arrival_time': {
                                                   '$lt': datetime.utcnow()}}).sort('arrival_time', pymongo.DESCENDING).limit(1)
    data = list(lastStop)
    data += list(scheduledStops)
    for stop in data:
        stop['all_trip_ids'] = list(
            db['gtfs_' + collectionPrefix].find({'block_id': stop['block_id']}).distinct('trip_id'))
        checkins = db['checkins'].find(
            {'tripId': {'$in': stop['all_trip_ids']}}).sort('time', pymongo.DESCENDING)
        for checkin in checkins:
            try:
                stop['adherence'] = checkin['adherence']
                stop['busId'] = checkin['busId']
                break
            except KeyError:
                pass
    return json.dumps(data, default=dthandler)


@app.route('/api/stop_times/<stopId>/')
@support_jsonp
def getBusesAtStop(stopId):
    return json.dumps(find_buses_at_stop(stopId), default=dthandler)


def find_buses_at_stop(stopId):
    print(stopId)
    scheduledStops = list(db['gtfs_' + collectionPrefix].find({'stop_id': stopId,
                                                               '$or': [
                                                                   {'arrival_time': {'$gte': datetime.utcnow() + timedelta(minutes=-5),
                                                                                     '$lte': datetime.utcnow() + timedelta(minutes=30)}},
                                                                   {'actual_arrival_time': {'$gte': datetime.utcnow() + timedelta(minutes=-5),
                                                                                            '$lte': datetime.utcnow() + timedelta(minutes=30)}}]
                                                               }).sort('arrival_time'))
    for stop in scheduledStops:
        stop['destination'] = db['destinations_' +
                                 collectionPrefix].find_one({'tripId': stop['trip_id']})['stopName']
        stop['all_trip_ids'] = list(
            db['gtfs_' + collectionPrefix].find({'block_id': stop['block_id']}).distinct('trip_id'))
        stop['_id'] = str(stop['_id'])

        routeDetails = db[
            'routes_' + collectionPrefix].find_one({'route_id': stop['route_id']})
        stop['routeLongName'] = routeDetails['route_long_name']
        stop['routeShortName'] = routeDetails['route_short_name']
        stop['routeDescription'] = routeDetails['route_desc']
        stop['routeType'] = routeDetails['route_type']

        checkins = db['checkins'].find(
            {'tripId': {'$in': stop['all_trip_ids']}}).sort('time', pymongo.DESCENDING)
        for checkin in checkins:
            try:
                stop['busAdherence'] = checkin['adherence']
                stop['busId'] = checkin['busId']
                stop['busPosition'] = checkin['location']
                stop['busCheckinTime'] = checkin['time']
                break
            except KeyError:
                pass
        stop.pop('all_trip_ids')
    return scheduledStops


@app.route('/api/v2/stops/near/<lat>/<lng>/')
@support_jsonp
def get_stops_near(lat=None, lng=None, cap=6):
    """
    @lat - Lattitude Value  (36.850769)
    @lng - Longitude Value fmt (-76.285873)
    @cap - Cap = number of values to return
    """
    stops = db['stops_' + collectionPrefix].find(
        {"location": {"$near": [float(lat), float(lng)]}}).limit(cap)
    stops = list(stops)
    for stop in stops:
        stop['_id'] = str(stop['_id'])
        stop['buses'] = find_buses_at_stop(stop.get('stopId'))

    return json.dumps(stops, default=json_util.default)


@app.route('/api/v2/stops')
@support_jsonp
def get_buses_at_stop():
    """
    Similar to the stop-times function but for the v2 api
    """
    # Unique Sets Please
    stop_ids = set(request.args.get("id").split(','))
    return json.dumps([find_buses_at_stop(stop_id) for stop_id in stop_ids], default=dthandler)

if __name__ == '__main__':
    # Bind to PORT if defined, otherwise default to 5000.
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=True)
