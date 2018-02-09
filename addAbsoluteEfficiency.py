#  -*- coding: utf-8 -*-
import sys
import datetime
from MongoDBConnect import *
from MongoDB import *
from queries import *
from processTweet import *

if __name__ == "__main__":
    print ("Start RSA: {0}".format(datetime.datetime.now()))
   
    connectMongoDB = Connect2MongoDB('localhost', 27017)
    connectMongoDB.setDB('test1') 
    db = MongoDB(connectMongoDB)

    tweets = db.find(MongoDB.CLEAR_TWEETS_COLLECTION, {CONST_RATIO_SUCCESS_ABSOLUTE: { "$exists" : False}})
    #Inside of MongoDB.find() --> 0 = MongoDB.TWEETS_COLLECTION
    #tweet = db.find(0, getTweet("id_str",'917946195410128897'))


    bulk = connectMongoDB.getCollection(MongoDB.CLEAR_TWEETS_COLLECTION).initialize_unordered_bulk_op()
    for t in tweets:
        visibilityValue = t[CONST_VISIBILITY_VALUE]
        rtCount = t[ProcessTweet.CONST_RT_COUNT]
        replyCount = t[ProcessTweet.CONST_REPLY_COUNT]
        quoteCount = t[ProcessTweet.CONST_QUOTE_COUNT]

        valueRT = ProcessTweet.CONST_RT_VALUE * (rtCount / visibilityValue)
        valueReply = ProcessTweet.CONST_REPLY_VALUE * (replyCount / visibilityValue)
        valueQuote = ProcessTweet.CONST_QUOTE_VALUE * (quoteCount / visibilityValue)

        bulk.find({CONST_TWEET_ID:t[CONST_TWEET_ID]}).update({"$set":{CONST_RATIO_SUCCESS_ABSOLUTE: valueRT + valueReply + valueQuote}})

    bulk.execute()


    print ("Stop RSA: {0}".format(datetime.datetime.now()))