#  -*- coding: utf-8 -*-
import sys
import datetime
from MongoDBConnect import *
from MongoDB import *
from queries import *

if __name__ == "__main__":
    print ("Start: {0}".format(datetime.datetime.now()))
   
    connectMongoDB = Connect2MongoDB('localhost', 27017)
    connectMongoDB.setDB('test1') 
    db = MongoDB(connectMongoDB)

    tweets = db.find(MongoDB.CLEAR_TWEETS_COLLECTION)
    #Inside of MongoDB.find() --> 0 = MongoDB.TWEETS_COLLECTION
    #tweet = db.find(0, getTweet("id_str",'917946195410128897'))


    bulk = connect2MongoDB.getCollection(MongoDB.CLEAR_TWEETS_COLLECTION).initialize_unordered_bulk_op()
    for t in tweets:
        aux = () / t["visibility_value"]
        bulk.find({"tweetId":t["tweetId"]}).update({"$set":{"RS_Absolute":}})


    print ("Stop: {0}".format(datetime.datetime.now()))


    """
    bulk = db.testdata.initialize_unordered_bulk_op()
    for i in range (0, len(ids)):
        bulk.find( { '_id':  ids[i]}).update({ '$set': {  "isBad" : "N" }})
    print bulk.execute()

    db.test.update({"a":5},{$set:{"e":2}}, false, false)
    """