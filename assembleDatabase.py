import urllib3
from bs4 import BeautifulSoup
import wget
import time
from pathlib import Path
import os
import psycopg2
import datetime
import re
from tqdm import tqdm
from geopy.geocoders import Nominatim

from geojson import Feature, FeatureCollection, Point, dump

class getUFO():

    def downloadSightings(self):
        #Download root directory for sightings
        rooturl = 'http://www.nuforc.org/webreports/'
        req  = urllib3.PoolManager()
        page = req.request('GET', rooturl+'ndxevent.html')
        soup = BeautifulSoup(page.data, 'html.parser')

        #Parse through all the individual directories
        linklist = soup.find_all('a')
        linklist.remove(linklist[0]) #First entry goes back to home page

        for link in linklist:
                print(rooturl + link.get('href'))
                source = 'http://www.nuforc.org/webreports/' + link.get('href')
                dest = Path(r"C:\Users\Matth\Dropbox\personal\general\ufo_new\html_files") / link.get('href')
                wget.download(source, str(dest))

                time.sleep(1) #Don't overload the server


    def fillDatabase(self):
        #Fill postgreSQL database with entries from website

        conn = psycopg2.connect(database="postgres", user='postgres', password='SQlpassword6789', host='127.0.0.1', port= '5432')
        conn.autocommit = True

        #Creating a cursor object using the cursor() method
        cursor = conn.cursor()

        n = 0;
        fields = [field for field in os.listdir("html_files") if field.endswith(".html")]

        date, city, state, shape, duration, description, posted = '','','','', '', '', ''

        index = 0
        for field in fields:
            obj = open(Path(r"html_files") / field, 'r').read()
            soup = BeautifulSoup(obj, features = 'html.parser')
            n = -1

            #Pull out each entry in the html file
            for entry in tqdm(soup.find_all('td')):
                n = n+1
                if n%7==0:
                      #onvert date to the acceptable format
                      try:
                          dateInput = entry.get_text()
                          date = datetime.datetime.strptime(dateInput, "%m/%d/%y %H:%M")
                      except ValueError:
                          #This is a little cludge, but it matches the variability in formats in the data set.
                          dateInput = dateInput + " 0:00"
                          date = datetime.datetime.strptime(dateInput, "%m/%d/%y %H:%M")
                elif n%7 ==1:
                      city = entry.get_text()
                elif n%7 ==2:
                      state = entry.get_text()
                elif n%7 ==3:
                      shape = entry.get_text()
                elif n%7 ==4:
                      duration = entry.get_text()
                elif n%7 ==5:
                      description = entry.get_text()
                elif n%7 ==6:
                      posted = entry.get_text()
                      #On the end of a row push this as a new entry to our database

                      stmnt = '''INSERT INTO SIGHTINGS(ID, DATE_TIME, CITY, STATE, LONGITUDE, LATITUDE, SHAPE, DURATION, SUMMARY) VALUES (%s, %s, %s,%s, %s, %s, %s, %s, %s)'''

                      data = [index, date.strftime("%Y-%m-%d %H:%M:00"), city, state, -1, -1, shape, duration, description]

                      cursor.execute(stmnt, data)

                      index += 1
        #Close connection to database
        print("\n All records inserted into database!\n")
        conn.close()


    def getPosition(self):
        #Access database
        conn = psycopg2.connect(database="postgres", user='postgres', password='SQlpassword6789', host='127.0.0.1', port= '5432')
        conn.autocommit = True
        cursor = conn.cursor()

        #Access googleV3 to get lat/longitude of each sighting
        g = Nominatim(user_agent = "Jemielita")

        #Pull relevant data
        cursor.execute('''SELECT ID, CITY, STATE, LONGITUDE, LATITUDE from SIGHTINGS where LATITUDE= -1''')
        sightlist = cursor.fetchall()

        #Cycle through all sightings
        for sighting in sightlist:

            try:
                g = Nominatim(user_agent= "Jemielita")
                place, (lat, long) = g.geocode(sighting[1]+sighting[2])
            except:
                #Some error occured, set long and lat to -2, which will be our marker for this
                lat, long = -2,-2

                #Update this particular field
            sql = '''UPDATE SIGHTINGS SET LONGITUDE = %s, LATITUDE = %s WHERE ID = %s'''
            cursor.execute(sql, [lat, long, sighting[0]])
            print(lat,long)
            #time.sleep(0.5)

        conn.close()


    def cleanData(self):
        #Parse through database and add in a
        conn = psycopg2.connect(database="postgres", user='postgres', password='SQlpassword6789', host='127.0.0.1', port= '5432')
        conn.autocommit = True
        cursor = conn.cursor()

        cursor.execute('''SELECT ID, STATE, LONGITUDE, LATITUDE, USE_DATA from SIGHTINGS''')
        sightlist = cursor.fetchall()
        i = 0
        for sighting in sightlist:
            (lat, long) = sighting[2], sighting[3]
            if lat ==-1 or lat==-2:
                sql = '''UPDATE SIGHTINGS SET USE_DATA= FALSE WHERE ID = %s'''
                cursor.execute(sql, [sighting[0]])
                i+=1
            if long==-1 or long==-2:
                sql = '''UPDATE SIGHTINGS SET USE_DATA= FALSE WHERE ID = %s'''
                cursor.execute(sql, [sighting[0]])
                i+=1
        print('Number of altered entries = ' + str(i))
        conn.close

    def convertToGeoJson(self):
        #Convert data to GeoJSON so it can be rendered on website

        conn = psycopg2.connect(database="postgres", user='postgres', password='SQlpassword6789', host='127.0.0.1', port= '5432')
        conn.autocommit = True
        cursor = conn.cursor()

        cursor.execute('''SELECT * from SIGHTINGS WHERE USE_DATA = True''')
        sightlist = cursor.fetchall()

        features = []
        i = 0
        for sighting in sightlist:
            thispoint = Point((sighting[5], sighting[4]))
            features.append(Feature(geometry = thispoint, propererties= {"Description": sighting[7]}))
            i+=1
            if(i>1000):
                break
        feature_collection = FeatureCollection(features)

        with(open('ufo.geojson', 'w')) as outputfile:
            dump(feature_collection, outputfile)
        print('All sightings transfered to geojson file')
        conn.close()
