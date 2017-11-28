class MongoDB ():
    TWEETS_COLLECTION = 'tweets' # 0
    CLEAR_TWEETS_COLLECTION = 'clearTweet' # 1

    def __init__(self, connect2MongoDB):
        self.connect2MongoDB = connect2MongoDB
        self.tweets = connect2MongoDB.getCollection(MongoDB.TWEETS_COLLECTION)
        self.clearTweet = connect2MongoDB.getCollection(MongoDB.CLEAR_TWEETS_COLLECTION)

    def find (self, collection, query = None):
        if (collection == 0 or collection == MongoDB.TWEETS_COLLECTION):
            return self.connect2MongoDB.getCollection(MongoDB.TWEETS_COLLECTION).find(query)

        elif collection == 1 or collection == MongoDB.CLEAR_TWEETS_COLLECTION:
            return self.connect2MongoDB.getCollection(MongoDB.CLEAR_TWEETS_COLLECTION).find(query)

        return None

    def insert_one (self, collection, data = None):
        if data is not None:
            if (collection == 0 or collection == MongoDB.TWEETS_COLLECTION):
                self.connect2MongoDB.getCollection(MongoDB.TWEETS_COLLECTION).insert_one(data)
                return True

            elif collection == 1 or collection == MongoDB.CLEAR_TWEETS_COLLECTION:
                self.connect2MongoDB.getCollection(MongoDB.CLEAR_TWEETS_COLLECTION).insert_one(data)
                return True

            else:
                return False
        
        else:
            return False

    def update_one (self, collection, idlabel, idvalue, data):
        if data is not None and idvalue is not None and idlabel is not None:
            if (collection == 0 or collection == MongoDB.TWEETS_COLLECTION):
                self.connect2MongoDB.getCollection(MongoDB.TWEETS_COLLECTION).update_one (
                    {idlabel : idvalue},
                    {"$set": data},
                    upsert = False
                )
                return True

            elif collection == 1 or collection == MongoDB.CLEAR_TWEETS_COLLECTION:
                self.connect2MongoDB.getCollection(MongoDB.CLEAR_TWEETS_COLLECTION).update_one (
                    {idlabel : idvalue},
                    {"$set": data},
                    upsert = False
                )
                return True

            else:
                return False
        
        else:
            return False