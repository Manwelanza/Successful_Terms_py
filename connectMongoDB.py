#  -*- coding: utf-8 -*-
import re
import sys
from pymongo import MongoClient
cliente = MongoClient('localhost', 27017)
db = cliente['test1']

def getTweet (id_str):
    cursor = (db.tweets
                .find(
                    {"id_str": id_str}
                )
            )
    return cursor

tweet = getTweet('917946195410128897')
print(sys.getdefaultencoding())
print (tweet.count())
for t in tweet:
    print (t["text"])