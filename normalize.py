#  -*- coding: utf-8 -*-
import sys
import datetime
from MongoDBConnect import *
from MongoDB import *
from queries import *
from processTweet import *

def getFirstSorted (dbMongo, metric, sort):
    tweets = dbMongo.getSortV2(MongoDB.CLEAR_TWEETS_COLLECTION, metric, (metric, sort), 1)
    if tweets.count() > 0:
        return tweets[0][metric]
    else:
        return None

def getMax (dbMongo, metric):
    return getFirstSorted (dbMongo, metric, -1)

def getMin (dbMongo, metric):
    return getFirstSorted (dbMongo, metric, 1)

def normalize (dbMongo):
    print ("\nStart Normalize: {0}".format(datetime.datetime.now()))
    listMetric = [CONST_RATIO_SUCCESS, CONST_RATIO_SUCCESS_ABSOLUTE, CONST_VISIBILITY_VALUE, CONST_MUFFLED_DISCUSSION, CONST_PURE_DISCUSSION]

    for metric in listMetric:
        max = getMax (dbMongo, metric)
        min = getMin (dbMongo, metric)

        print ('Max (' + metric + '): ' + str(max))
        print ('Min (' + metric + '): ' + str(min))

    print ("\nStop Normalize: {0}".format(datetime.datetime.now()))


if __name__ == "__main__":
    connectMongoDB = Connect2MongoDB('localhost', 27017)
    connectMongoDB.setDB('test1') 
    db = MongoDB(connectMongoDB)

    normalize(db)
