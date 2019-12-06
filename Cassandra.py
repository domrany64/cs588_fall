from cassandra.cluster import Cluster

def initiate():
    cluster = Cluster(['0.0.0.0'],port=9042)
    session = cluster.connect('cs588damon',wait_for_all_pools=True)
    session.request_timeout=60000
    session.default_timeout=60000
    session.read_request_timeout=60000
    session.execute('USE cs588damon')
    return session

def query1(session):
    print("Calculating the answer of question 1")
    print("_____________________________________________________________")
    row = session.execute('SELECT count(*) FROM freeway_loopdata_oneday  WHERE speed > 100 ALLOW FILTERING')
    print("The number of speed violations which are above 100 mph: %s" %(row[0][0]))
    print("_____________________________________________________________")

def query2(session):
    print("Calculating the answer of question 2")
    print("_____________________________________________________________")
    detectors = session.execute('SELECT * FROM freeway_detectors WHERE locationtext = \'Foster NB\' ALLOW FILTERING')
    detector_id = []
    for detector in detectors:
        detector_id.append(detector.detectorid)
    total_v = session.execute('SELECT count(*) FROM freeway_loopdata WHERE detectorid in (%s) AND starttime >= \'2011-09-20 11:59:40\' AND starttime < \'2011-09-22 00:00:00\'  ALLOW FILTERING' %(', '.join(map(str,detector_id))));
    print("The total volumn for the statios FOSTER NB on the mentioned date: %s" %(total_v[0][0]))
    print("_____________________________________________________________")

def query3(session):
    print("Calculating the answer of question 3")
    print("_____________________________________________________________")

    print("_____________________________________________________________")

def query4(session):
    print("Calculating the answer of question 4")
    print("_____________________________________________________________")
    station = session.execute('SELECT * FROM freeway_stations WHERE locationtext = \'Foster NB\' ALLOW FILTERING')
    detectors = session.execute('SELECT * FROM freeway_detectors WHERE locationtext = \'Foster NB\' ALLOW FILTERING')
    detector_id = []
    for detector in detectors:
        detector_id.append(detector.detectorid)
    AVG1 = session.execute('SELECT avg(speed) FROM freeway_loopdata WHERE detectorid in (%s) AND speed > 5 AND starttime > \'2011-09-22 07:00:00\' AND starttime  < \'2011-09-22 09:00:00\' ALLOW FILTERING' %(', '.join(map(str,detector_id))))
    AVG2 = session.execute('SELECT avg(speed) FROM freeway_loopdata WHERE detectorid in (%s) AND speed > 5 AND starttime > \'2011-09-22 16:00:00\' AND starttime  < \'2011-09-22 18:00:00\' ALLOW FILTERING' %(', '.join(map(str,detector_id))))
    AVGspeed = (AVG1[0][0] + AVG2[0][0])/ 2
    print("The average speed: %s" %AVGspeed)
    print("Average travel time in seconds: %s" %(((station[0].length)/AVGspeed)*3660))
    print("_____________________________________________________________")

def query5(session):
    print("Calculating the answer of question 5")
    print("_____________________________________________________________")
    highway = session.execute('SELECT highwayid FROM highways WHERE highwayname = \'I-205\' AND shortdirection = \'N\' ALLOW FILTERING')
    hwy_id = highway[0].highwayid
    print("Creating index on highway id for stations table")
    session.execute('CREATE INDEX h_id ON freeway_stations (highwayid)')
    I205N_length = session.execute('select SUM(length) FROM freeway_stations WHERE highwayid = %s Allow FILTERING' %(hwy_id) )
    print("Creating index om highway id for detectors table")
    session.execute('CREATE INDEX h_idx ON freeway_detectors (highwayid)')
    detectors = session.execute('SELECT detectorid FROM freeway_detectors WHERE highwayid = %s' %(hwy_id))
    detector_id = []
    for detector in detectors:
        detector_id.append(detector.detectorid)
    AVG1 = session.execute('SELECT avg(speed) FROM freeway_loopdata WHERE detectorid in (%s) AND speed > 5 AND starttime > \'2011-09-22 07:00:00\' AND starttime  < \'2011-09-22 09:00:00\' ALLOW FILTERING' %(', '.join(map(str,detector_id))))
    AVG2 = session.execute('SELECT avg(speed) FROM freeway_loopdata WHERE detectorid in (%s) AND speed > 5 AND starttime > \'2011-09-22 16:00:00\' AND starttime  < \'2011-09-22 18:00:00\' ALLOW FILTERING' %(', '.join(map(str,detector_id))))
    AVGspeed = (AVG1[0][0] + AVG2[0][0])/ 2
    print("The average speed: %s" %AVGspeed)
    print("Average travel time in minutes: %s" %(((I205N_length[0][0])/AVGspeed)*60))
    print("Dropping the highway id indices")
    session.execute('DROP INDEX h_id')
    session.execute('DROP INDEX h_idx')
    print("_____________________________________________________________")

def query6(session):
    print("Calculating the answer of question 6")
    print("_____________________________________________________________")

    print("_____________________________________________________________")


if __name__ == "__main__":
    session = initiate()
    #query1(session)
    #query2(session)
    #query4(session)
    query5(session)