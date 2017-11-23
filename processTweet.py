from queries import *
from inserts import *

class ProcessTweet ():
    CONST_RT = "retweeted_status"
    CONST_QUOTE = "quoted_status"
    CONST_REPLY = "in_reply_to_status_id_str"
    CONST_ID = "id_str"
    CONST_TRUNCATED = "truncated"
    CONST_FULL_TEXT = "full_text"
    CONST_TEXT = "text"
    CONST_EXTENDED_TWEET = "extended_tweet"
    
    def __init__ (self, dataBase):
        self.db = dataBase
       
    def defineType (self, tweet):
        if ProcessTweet.CONST_RT in tweet:
            self.isRT = True
        else:
            self.isRT = False

        if ProcessTweet.CONST_QUOTE in tweet:
            self.isQuote = True
        else:
            self.isQuote = False

        if tweet[ProcessTweet.CONST_REPLY] != None:
            self.isReply = True
        else:
            self.isReply = False

    def process (self, tweet):
        self.defineType(tweet)
        originalTweet = tweet
        if self.isRT:
            originalTweet = self.getOriginalTweet(originalTweet[ProcessTweet.CONST_RT])

        elif self.isQuote:
            originalTweet = self.getOriginalTweet(originalTweet)

        self.processNormalTweet (originalTweet)

    def getOriginalTweet(self, tweet):
        auxTweet = tweet
        while ProcessTweet.CONST_QUOTE in auxTweet:
            auxTweet = auxTweet[ProcessTweet.CONST_QUOTE]
        
        return auxTweet

    def processNormalTweet (self, tweet):
        tweetId = tweet[ProcessTweet.CONST_ID]
        if self.db.find(1, getTweet ("tweetId", tweetId)).count() == 0:
            data = getInsertNewClearTweet(tweetId)
            data["isQuote"] = self.isQuote
            data["isReply"] = self.isReply
            if tweet[ProcessTweet.CONST_TRUNCATED]:
                data["text"] = tweet[ProcessTweet.CONST_EXTENDED_TWEET][ProcessTweet.CONST_FULL_TEXT]
            else:
                data["text"] = tweet[ProcessTweet.CONST_TEXT]

            aux = self.db.insert_one(1, data)
    