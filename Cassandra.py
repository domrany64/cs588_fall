from cassandra.cluster import Cluster

def initiate():
    cluster = Cluster(['0.0.0.0'],port=9042)
    session = cluster.connect('cs588damon',wait_for_all_pools=True)
    session.request_timeout=60000
    session.default_timeout=60000
    session.read_request_timeout=60000
    session.execute('USE cs588damon')
    return session

def pprint(Qn,list1):
    texts = []
    if (Qn == 1):
        texts.append("The number of speed violations which are above 100 mph: ")
    elif (Qn == 2):
        texts.append("The total volumn for the statios FOSTER NB on the mentioned date: ")
    elif (Qn == 3):
        texts.append("")
    elif (Qn == 4):
        texts.append("The average speed: ")
        texts.append("Average travel time in seconds: ")
    elif (Qn == 5):
        texts.append("The average speed: ")
        texts.append("Average travel time in minutes: ")
    elif (Qn == 6):
        texts.append("")
    print("Calculating the answer of question %s" %Qn)
    for text,var in zip(texts,list1):
        print(text+str(var))
    print("_____________________________________________________________")



def query1(session,data,speed):
    row = session.execute('SELECT count(*) FROM %s  WHERE speed > %s ALLOW FILTERING' %(data,speed))
    return row[0][0]

def query2(session,data,station_name):
    detectors = session.execute('SELECT * FROM freeway_detectors WHERE locationtext = \'%s\' ALLOW FILTERING' %station_name)
    detector_id = []
    for detector in detectors:
        detector_id.append(detector.detectorid)
    total_v = session.execute('SELECT SUM(volume) FROM %s WHERE detectorid in (%s) AND starttime >= \'2011-09-20 11:59:40\' AND starttime < \'2011-09-22 00:00:00\'  ALLOW FILTERING' %(data,(', '.join(map(str,detector_id)))));
    return total_v[0][0]

#def query3(session):


def query4(session, data, station_name):
    station = session.execute('SELECT * FROM freeway_stations WHERE locationtext = \'%s\' ALLOW FILTERING' %station_name)
    detectors = session.execute('SELECT * FROM freeway_detectors WHERE locationtext = \'%s\' ALLOW FILTERING' %station_name)
    detector_id = []
    for detector in detectors:
        detector_id.append(detector.detectorid)
    AVG1 = session.execute('SELECT avg(speed) FROM %s WHERE detectorid in (%s) AND speed > 5 AND starttime > \'2011-09-22 07:00:00\' AND starttime  < \'2011-09-22 09:00:00\' ALLOW FILTERING' %(data,(', '.join(map(str,detector_id)))))
    AVG2 = session.execute('SELECT avg(speed) FROM %s WHERE detectorid in (%s) AND speed > 5 AND starttime > \'2011-09-22 16:00:00\' AND starttime  < \'2011-09-22 18:00:00\' ALLOW FILTERING' %(data,(', '.join(map(str,detector_id)))))
    AVGspeed = (AVG1[0][0] + AVG2[0][0])/ 2
    if (AVGspeed != 0):
        AVGtimeSEC = (((station[0].length)/AVGspeed)*3600)
    else:
        AVGtimeSEC = 0
    return (AVGspeed,AVGtimeSEC)

def query5(session, data, hwy_name,hwy_dir):
    highway = session.execute('SELECT highwayid FROM highways WHERE highwayname = \'%s\' AND shortdirection = \'%s\' ALLOW FILTERING' %(hwy_name,hwy_dir))
    hwy_id = highway[0].highwayid
    station_names = session.execute('SELECT locationtext FROM freeway_stations WHERE highwayid = %s ALLOW FILTERING' %hwy_id)
    AVGtime = 0
    for station_name in station_names:
        (AVGspeed,AVGtimeSEC) = query4(session, data, station_name)
        AVGtime += AVGtimeSEC
    return (AVGtime/60)


#def query6(session):


if __name__ == "__main__":
    session = initiate()
    q1 = [query1(session,"freeway_loopdata_onehour",100)]
    pprint(1,q1)
    q2 = [query2(session,"freeway_loopdata","Foster NB")]
    pprint(2,q2)
    (AVGspeed,AVGtimeSEC) = query4(session,"freeway_loopdata", 'Foster NB')
    pprint(4,[AVGspeed,AVGtimeSEC])
    q5 = [query5(session, "freeway_loopdata","I-205","N")]
    pprint(5,q5)