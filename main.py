#  -*- coding: utf-8 -*-
import sys
from MongoDBConnect import *
from MongoDB import *
from queries import *
from processTweet import *
from Neo4JDBConnect import *
from Neo4JDB import *
from credentials import *

if __name__ == "__main__":
    connectMongoDB = Connect2MongoDB('localhost', 27017)
    connectMongoDB.setDB('test1') 
    db = MongoDB(connectMongoDB)
    graph = Neo4jDB (Connect2Neo4J (CONST_NEO4J_URI, CONST_NEO4J_USER, CONST_NEO4J_PASSWORD))

    tweets = db.find(MongoDB.TWEETS_COLLECTION)
    #Inside of MongoDB.find() --> 0 = MongoDB.TWEETS_COLLECTION
    #tweet = db.find(0, getTweet("id_str",'917946195410128897'))



    process = ProcessTweet (db, graph)
    for t in tweets:
        process.process(t)


    graph.connect2Neo4J.closeDB()