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

    tweets = db.find(MongoDB.TWEETS_COLLECTION)
    #Inside of MongoDB.find() --> 0 = MongoDB.TWEETS_COLLECTION
    #tweet = db.find(0, getTweet("id_str",'917946195410128897'))



    process = ProcessTweet (db)
    for t in tweets:
        process.process(t)


