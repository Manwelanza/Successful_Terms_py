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
    CONST_QUOTE_STATUS = "is_quote_status"
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
    CONST_URLS = "urls"
    
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
        clearTweets = self.db.find(1, getTweet ("tweetId", tweetId))
        if clearTweets.count() == 0:
            self.db.insert_one(1, getInsertClearTweet(tweet, isQuote, isReply, self))
        else:
            print ("update process")
            updateFields = []
            print ("Saved RT {0} -- New RT {1}".format(clearTweets[0][ProcessTweet.CONST_RT_COUNT], tweet[ProcessTweet.CONST_RT_COUNT]))
            if clearTweets[0][ProcessTweet.CONST_RT_COUNT] < tweet[ProcessTweet.CONST_RT_COUNT]:
                updateFields.append (ProcessTweet.CONST_RT_COUNT)
            
            print ("Saved FAV {0} -- New FAV {1}".format(clearTweets[0][ProcessTweet.CONST_FAVORITE_COUNT], tweet[ProcessTweet.CONST_FAVORITE_COUNT]))
            if clearTweets[0][ProcessTweet.CONST_FAVORITE_COUNT] < tweet[ProcessTweet.CONST_FAVORITE_COUNT]:
                updateFields.append (ProcessTweet.CONST_FAVORITE_COUNT)

            print ("Saved QT {0} -- New QT {1}".format(clearTweets[0][ProcessTweet.CONST_QUOTE_COUNT], tweet[ProcessTweet.CONST_QUOTE_COUNT]))
            if clearTweets[0][ProcessTweet.CONST_QUOTE_COUNT] < tweet[ProcessTweet.CONST_QUOTE_COUNT]:
                updateFields.append (ProcessTweet.CONST_QUOTE_COUNT)

            if len(updateFields) > 0:
                print("update done")
                self.db.update_one(1, "tweetId", tweetId, updateClearTweets(tweet, updateFields))
            

    def process (self, tweet):
        auxTweet = tweet
        if ProcessTweet.CONST_RT in tweet:
            # TODO: Do something with RTs
            auxTweet = auxTweet[ProcessTweet.CONST_RT]

        while ProcessTweet.CONST_QUOTE in auxTweet and auxTweet[ProcessTweet.CONST_QUOTE_STATUS] is True:
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

    