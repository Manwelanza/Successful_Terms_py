#  -*- coding: utf-8 -*-
import re
import sys
from pymongo import MongoClient
import pprint

CONST_RT = "retweeted_status"
CONST_QUOTE = "quoted_status"

cliente = MongoClient('localhost', 27017)
db = cliente['test1']
clearTweet = client['test1'].clearTweet

def getTweet (id_str):
    cursor = (db.tweets
                .find(
                    {"id_str": id_str}
                )
            )
    return cursor

def getAllTweet ():
    return db.tweets.find()

def getRTs ():
    return (db.tweets
                .find(
                    {"retweeted_status": { "$exists" : True}}
                )
            )

def getQuotes ():
    return (db.tweets
                .find(
                    {"quoted_status": { "$exists" : True}}
                )
            )

def getOriginals ():
    return (db.tweets
                .find(
                    {"$and": [
                        {"quoted_status": { "$exists" : False}},
                        {"retweeted_status": { "$exists" : False}}
                    ]}
                )
            )

def getReplys ():
    return (db.tweets
                .find(
                    {"in_reply_to_status_id_str": {"$ne": None}}
                )
            )

def processRT(tweet):
    originalTweet = getOriginalTweet(tweet[CONST_RT])

    if clearTweet.find({"tweetId": originalTweet["id_str"]}).count() == 0:
        insertTweet(originalTweet)

    
    
def getOriginalTweet(tweet):
    if CONST_QUOTE in tweet:
        return getOriginalTweet(tweet[CONST_QUOTE])
    else:
        return tweet

def processQuote(tweet, isRT):
    pass

def insertTweet (tweet):
    pass

"""
tweet = getTweet('917946195410128897')
print(sys.getdefaultencoding())
print (tweet.count())
for t in tweet:
    print (t["text"])
"""
RTcount = 0
quoteCount = 0
tweets = getAllTweet()
rts = getRTs()
quotes = getQuotes()
originals = getOriginals()
replys = getReplys()

for t in tweets:
    isRT = False
    if CONST_RT in t:
        isRT = True
        RTcount += 1
        processRT(t)
    
    if CONST_QUOTE in t:
        quoteCount += 1
        processQuote(t, isRT)

print ("{0} RTs of {1} tweets".format(RTcount, tweets.count()))
print ("{0} quotes of {1} tweets".format(quoteCount, tweets.count()))
print ("{0} original tweets".format(tweets.count() - RTcount - quoteCount))
print ("{0} RTs".format(rts.count()))
print ("{0} Quotes".format(quotes.count()))
print ("{0} Originals".format(originals.count()))
print ("{0} replys".format(replys.count()))