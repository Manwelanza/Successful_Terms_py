class MongoDB ():
    TWEETS_COLLECTION = 'tweets'
    CLEAR_TWEETS_COLLECTION = 'clearTweet'

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
    