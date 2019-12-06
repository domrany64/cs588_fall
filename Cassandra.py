from cassandra.cluster import Cluster

def initiate():
    cluster = Cluster(['0.0.0.0'],port=9042)
    session = cluster.connect('cs588damon',wait_for_all_pools=True)
    session.request_timeout=60000
    session.default_timeout=60000
    session.read_request_timeout=60000
    session.execute('USE cs588damon')
    return session

def query1():
    print("Calculating the answer of question 1")
    print("_____________________________________________________________")
    rows = session.execute('SELECT * FROM freeway_loopdata  WHERE speed > 100 ALLOW FILTERING')
    for row in rows:
        print(row.speed)
    print("_____________________________________________________________")

def query2():
    print("Calculating the answer of question 2")
    print("_____________________________________________________________")

    print("_____________________________________________________________")

def query3():
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
    print("Average travel time in second: %s" %(((station[0].length)/AVGspeed)*3660))
    print("_____________________________________________________________")

def query5():
    print("Calculating the answer of question 5")
    print("_____________________________________________________________")

    print("_____________________________________________________________")

def query6():
    print("Calculating the answer of question 6")
    print("_____________________________________________________________")

    print("_____________________________________________________________")


if __name__ == "__main__":
    session = initiate()
    query4(session)