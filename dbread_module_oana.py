#
# dbread_module_oana.py
#
# Description: function to read data from the database tables
#
# 18 Octombrie 2016
#
# Andrei Diamandi
#

import traceback
import sys
import os
import csv

from datetime import datetime, date, time
#from psycopg2 import datetime, date, time
from os import listdir
from os.path import isfile, join
import psycopg2
import numpy as np    
   
def ReadSynopTable():

    import time
    start_time = time.time()
    # Connect to the existing Fog database
    try:
        conn = psycopg2.connect("dbname=fog1 user=andrei password=andrei123")
    except:
        print "I am unable to connect to the database"
    # Open a cursor to perform database operations
    cur = conn.cursor()
    
    cur.execute("SELECT COUNT(geo_time) OVER (),geopge02.station_name, geopge02.geo_time, synop5.synop_time, geopge02.ct[4], \
                synop5.ca, synop5.synop_date FROM geopge02 INNER JOIN synop5 ON \
                (synop5.block_no=geopge02.block_no) AND (synop5.station_no=geopge02.station_no) AND (geo_date>=date '2015-04-01') \
                AND (geo_date <= date '2016-04-30') AND (geo_date=synop_date) \
                AND (geo_time=synop_time) AND ((geopge02.ct[4]<>1) AND (ca=0));")

    
    hist_ca = np.zeros((64), dtype=np.int)
    hist_ct = np.zeros((64), dtype=np.int)
    i = 0
    
    for record in cur:
        #print record[0]
        bin = int(record[5])
        #print("record[5]", record[5])
        hist_ca[bin] +=1
        #print(hist_ca[bin])
        bin = int(record[4])
        hist_ct[bin] +=1
        #print("record[3]", record[3])
        #print(hist_ct[bin])      
        i +=1      

    print('hist_ca', hist_ca)
    print('hist_ct', hist_ct)
    print(i)
    print("cur rows=", cur.rowcount) # Returns the number of rows returned 
    # Close communication with the database
    cur.close()
    conn.close()
    print("--- %s seconds ---" % (time.time() - start_time))





def SynGeoQuery():

    import time
    start_time = time.time()
    
    # Connect to the existing Fog database
    try:
        conn = psycopg2.connect("dbname=fog1 user=andrei password=andrei123")
    except:
        print "I am unable to connect to the database"
    # Open a cursor to perform database operations
    cur = conn.cursor()

    cur.execute("""SELECT COUNT(geo_time) OVER (), geopge02.station_name, geopge02.geo_time, synop5.synop_time, synop5.ct_low, synop5.ca, synop5.synop_date \
                FROM geopge02 INNER JOIN synop5 ON \
                (synop5.block_no=geopge02.block_no) AND (synop5.station_no=geopge02.station_no) AND (geo_date>=date '2015-04-01') \
                AND (geo_date <= date '2016-04-30') AND (geo_date=synop_date) AND (geo_time=synop_time) \
                AND (geopge02.ct[4]>=1 AND geopge02.ct[5] <=4 AND synop5.ca<=2);""")    
    
    
    print("cur rows=", cur.rowcount)
    
    print("am ajuns pana aici...in SynGeoQUery")
    
    print("cur rows=", cur.rowcount)

    # Close communication with the database
    cur.close()
    conn.close()
    
    print("--- %s seconds ---" % (time.time() - start_time))
    



def ReadGeoPge02Table():

    import time
    start_time = time.time()
        

    # Connect to the existing Fog database
    try:
        conn = psycopg2.connect("dbname=fog1 user=andrei password=andrei123")
    except:
        print "I am unable to connect to the database"
    # Open a cursor to perform database operations
    cur = conn.cursor()
    i = 0
    cloud_free = 0

    cur.execute("""SELECT count(*) AS exact_count FROM synop5;""")
   

    print("cur rows=", cur.rowcount)
    print("am ajuns pana aici...")
    match = 0
    #for record in cur:
        #print(record[0], record[1])
        #if (record[3][5] == 1) and (record[4] == 0):
        #    match += 1
        #   cloud_free += 1
        #i +=1

    # how many times the do not match, i.e. synop reports clear sky and GEO something else ?
            
    cur.execute("""SELECT geopge02.station_name, geopge02.geo_time, synop5.synop_time, geopge02.ct[5], synop5.ca, synop5.synop_date \
                FROM geopge02, synop5 \
                WHERE (geo_date>=date '2015-04-01') AND (geo_date < date '2015-04-03') AND (synop5.synop_date>=date '2015-04-01') \
                AND (geo_date < date '2015-04-03') \
                AND (geo_date=synop5.synop_date) AND (geo_time=synop5.synop_time) AND ((geopge02.ct[5]<>1) AND (ca=0));""")    

    print("cur rows=", cur.rowcount)


    # Close communication with the database
    cur.close()
    conn.close()
    print(i)
    print("cloud_free=", cloud_free)
    print("match =", match)
    print("--- %s seconds ---" % (time.time() - start_time))
