from queries import *
from inserts import *
from updates import *

class ProcessTweet ():
    CONST_RT = "retweeted_status"
    CONST_QUOTE = "quoted_status"
    CONST_REPLY = "in_reply_to_status_id_str"
    CONST_ID = "id_str"
    CONST_TRUNCATED = "truncated"
    CONST_FULL_TEXT = "full_text"
    CONST_TEXT = "text"
    CONST_EXTENDED_TWEET = "extended_tweet"
    CONST_REPLY_TO_STATUS_ID = "in_reply_to_status_id_str"
    CONST_QUOTE_TO_STATUS_ID = "quoted_status_id_str"
    CONST_ENTITIES = "entities"
    CONST_DISPLAY_TEXT_RANGE = "display_text_range"
    CONST_HASHTAGS = "hashtags"
    CONST_USER_MENTIONS = "user_mentions"
    CONST_SYMBOLS = "symbols"
    CONST_MEDIA = "media"
    CONST_REPLY_COUNT = "reply_count"
    CONST_RT_COUNT = "retweet_count"
    CONST_QUOTE_COUNT = "quote_count"
    CONST_FAVORITE_COUNT = "favorite_count"
    CONST_LANGUAGE = "lang"
    CONST_COORDINATES = "coordinates"
    CONST_USER = "user"
    CONST_CREATED_AT = "created_at"
    
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


    def processNormalTweet (self, tweet, isQuote, isReply):
        tweetId = tweet[ProcessTweet.CONST_ID]
        if self.db.find(1, getTweet ("tweetId", tweetId)).count() == 0:
            self.db.insert_one(1, getInsertClearTweet(tweet, isQuote, isReply, self))

    def process (self, tweet):
        auxTweet = tweet
        if ProcessTweet.CONST_RT in tweet:
            # TODO: Do something with RTs
            auxTweet = auxTweet[ProcessTweet.CONST_RT]

        while ProcessTweet.CONST_QUOTE in auxTweet:
            isReply = auxTweet[ProcessTweet.CONST_REPLY] != None
            self.processNormalTweet(auxTweet, isReply, True)
            auxTweet = auxTweet[ProcessTweet.CONST_QUOTE]

        isReply = auxTweet[ProcessTweet.CONST_REPLY] != None
        self.processNormalTweet(auxTweet, isReply, False)    

    def getOriginalTweet(self, tweet):
        auxTweet = tweet
        while ProcessTweet.CONST_QUOTE in auxTweet:
            auxTweet = auxTweet[ProcessTweet.CONST_QUOTE]
        
        return auxTweet

    