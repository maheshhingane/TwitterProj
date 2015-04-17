#!/usr/bin/env python

import sys
import MySQLdb
import twitter
import time
import datetime
import traceback

api = twitter.Api(consumer_key='iWYeAzK4wQ2IjGelxVXU6g',
                  consumer_secret='8KnwdPUSPz7h19JsP6rl4efDNjvZBX4vCmqTmbxpE6A', 
                  access_token_key='796634876-XmhAHoVy7qfwBoUZblSBMOrrGddkrR1P0zyqq1Yf', 
                  access_token_secret='c7vxAo5GtNgcpdHdSXTrWUF46pTZvhSSDfX9FUDjU')

try:
    conn = MySQLdb.connect(host= "localhost",
                       user="root",
                       passwd="root",
                       db="mydb")

    selectquery = conn.cursor()
    insertquery = conn.cursor()

    statuses = api.GetSearch(term='#bigdata')

    for s in statuses:
        selectquery.execute("SELECT COUNT(1) FROM mydb.tweetbank WHERE id = '%s'" % (s.GetId()))
        if(selectquery.fetchone()[0] == 0):
            insertquery.execute("""INSERT INTO mydb.tweetbank (created_at, from_user, geo, id, source, text, to_user) VALUES (%s, %s, %s, %s, %s, %s, %s)""", (s.GetCreatedAt(), str(s.GetUser()).split(':').pop(3).split('"').pop(1).encode('utf-8', 'replace'), str(s.GetGeo()).encode('utf-8', 'replace'), s.GetId(), s.GetSource(), s.GetText().encode('utf-8', 'replace'), str(s.GetInReplyToScreenName()).encode('utf-8', 'replace')))
            print "Inserted data in table mydb.tweetbank at ", str(datetime.datetime.now())

    insertquery.close()
    selectquery.close()

    conn.commit()

    conn.close()

except:
    print "Unexpected error:", traceback.format_exc()
