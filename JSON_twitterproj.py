#!/usr/bin/env python

import sys
import MySQLdb
import twitter
import time
import datetime
import traceback

import json
import urllib, urllib2


conn = MySQLdb.connect(host= "localhost",
                       user="root",
                       passwd="root",
                       db="mydb")

cursor = conn.cursor()

cursor.execute ("select min(rowid) from mydb.tweetbank")
min_rowid = cursor.fetchone()[0]
cursor.execute ("select max(rowid) from mydb.tweetbank")
max_rowid = cursor.fetchone()[0]
f1 = open('/home/mahesh/Desktop/Results/profilelocations.txt','w')
f2 = open('/home/mahesh/Desktop/Results/tweetlocations.txt','w')

for x in range(min_rowid, max_rowid):
	cursor.execute ("select id from mydb.tweetbank where rowid = %s", x)
	result = cursor.fetchall()
	if result:
		tweetid = str(urllib.quote(result[0][0]))

		try:
			url = 'https://api.twitter.com/1/statuses/show.json?id=' + tweetid + '&include_entities=true'
			print url
			req = urllib2.Request(url)
			response = urllib2.urlopen(req)
			the_page = response.read()
			userloc = json.loads(the_page)["user"]["location"].encode('utf-8')
			if isinstance(json.loads(the_page)["place"], dict):
				tweetloc = json.loads(the_page)["place"]["full_name"].encode('utf-8') + ", " + json.loads(the_page)["place"]["country"].encode('utf-8')
			else:
				tweetloc = "Not found"

			print str(x) + ">>>" + tweetid + ">>> userloc: " + userloc + ">>> tweetloc: " + tweetloc
			
			f1.write(userloc)
			f1.write('\n')

			f2.write(tweetloc)
			f2.write('\n')

			userloc = ""
			tweetloc = ""

		except:
			if "HTTPError: HTTP Error 400: Bad Request" in traceback.format_exc():
				print "Timeout reached, waiting for 1 hour. Current time: ", time.ctime()
				time.sleep(3600)
			else:
				print "Unexpected error:", traceback.format_exc()
	

cursor.close ()
conn.close()

