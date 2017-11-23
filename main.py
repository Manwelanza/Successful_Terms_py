#  -*- coding: utf-8 -*-
import sys
from MongoDBConnect import *
from MongoDB import *
from queries import *
from processTweet import *

if __name__ == "__main__":
    connectMongoDB = Connect2MongoDB('localhost', 27017)
    connectMongoDB.setDB('test1') 
    db = MongoDB(connectMongoDB)

    RTcount = 0
    quoteCount = 0
    tweets = db.find(MongoDB.TWEETS_COLLECTION)
    rts = db.find(MongoDB.TWEETS_COLLECTION, getRTs())
    quotes = db.find(0, getQuotes())
    originals = db.find(0, getNormalTweets())
    replys = db.find(0, getReplys())
    #Inside of MongoDB.find() --> 0 = MongoDB.TWEETS_COLLECTION
    tweet = db.find(0, getTweet("id_str",'917946195410128897'))
    print(sys.getdefaultencoding())
    print (tweet.count())
    process = ProcessTweet (db)
    for t in tweets:
        isRT = False
        process.process(t)
        if ProcessTweet.CONST_RT in t:
            isRT = True
            RTcount += 1
            #processRT(t)
        
        if ProcessTweet.CONST_QUOTE in t:
            quoteCount += 1
            #processQuote(t, isRT)

    print ("{0} RTs of {1} tweets".format(RTcount, tweets.count()))
    print ("{0} quotes of {1} tweets".format(quoteCount, tweets.count()))
    print ("{0} original tweets".format(tweets.count() - RTcount - quoteCount))
    print ("{0} RTs".format(rts.count()))
    print ("{0} Quotes".format(quotes.count()))
    print ("{0} Originals".format(originals.count()))
    print ("{0} replys".format(replys.count()))


