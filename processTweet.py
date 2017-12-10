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
    CONST_FOLLOWERS_COUNT = "followers_count"
    CONST_CREATED_AT = "created_at"
    CONST_URLS = "urls"

    CONST_RT_VALUE = 1
    CONST_QUOTE_VALUE = 1
    CONST_REPLY_VALUE = 0.5
    
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


    def processNormalTweet (self, tweet, isReply, isQuote):
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
        tweets = []
        if ProcessTweet.CONST_RT in tweet:
            # TODO: Do something with RTs
            tweets.append({"type":"RT", "tweet":auxTweet, "isReply":False})
            auxTweet = auxTweet[ProcessTweet.CONST_RT]
            

        while ProcessTweet.CONST_QUOTE in auxTweet and auxTweet[ProcessTweet.CONST_QUOTE_STATUS] is True:
            isReply = auxTweet[ProcessTweet.CONST_REPLY] != None
            tweets.append({"type":"quote", "tweet":auxTweet, "isReply":isReply})
            #self.processNormalTweet(auxTweet, isReply, True)
            auxTweet = auxTweet[ProcessTweet.CONST_QUOTE]

        isReply = auxTweet[ProcessTweet.CONST_REPLY] != None
        tweets.append({"type":"normal", "tweet":auxTweet, "isReply":isReply})
        #self.processNormalTweet(auxTweet, isReply, False)

        for t in reversed(tweets):
            self.auxProcess(t)
    
    def auxProcess (self, tweetInfo):
        if tweetInfo["type"] == "RT":
            self.processRT (tweetInfo["tweet"])
        elif tweetInfo["type"] == "quote":
            self.processNormalTweet(tweetInfo["tweet"], tweetInfo["isReply"], True)
        else:
            self.processNormalTweet(tweetInfo["tweet"], tweetInfo["isReply"], False)

    def processRT (self, tweet):
        toTweetId = tweet[ProcessTweet.CONST_RT][ProcessTweet.CONST_ID]
        updateFields=[]
        if toTweetId != None:
            value = tweet[ProcessTweet.CONST_USER][ProcessTweet.CONST_FOLLOWERS_COUNT] * ProcessTweet.CONST_RT_VALUE
            updateFields.append({"field":CONST_VISIBILITY_VALUE, "value":value, "type":"+"})
            updateFields.append({"field":CONST_VISIBILITY_COUNT_RT, "value":1, "type":"+"})
            #Crear metodo para solo actualizar clearTweet si no se ha contado ya el RT
            #update toTweetId with updateFields


    def processQuote (self, tweet, isReply):
        tweetId = tweet[ProcessTweet.CONST_ID]
        quoteId = tweet[ProcessTweet.CONST_QUOTE_TO_STATUS_ID]
        value2Quote =  ProcessTweet.CONST_QUOTE_VALUE
        replyId = None
        updateFieldsQuote=[]
        updateFieldsReply=[]
        updateFieldsTweet=[]

        if isReply:
            replyId = tweet[ProcessTweet.CONST_REPLY_TO_STATUS_ID]

        if quoteId != None and replyId != None and quoteId != replyId:
            value = tweet[ProcessTweet.CONST_USER][ProcessTweet.CONST_FOLLOWERS_COUNT] * ProcessTweet.CONST_REPLY_VALUE
            updateFieldsQuote.append({"field":CONST_VISIBILITY_VALUE, "value":value, "type":"+"})
            updateFieldsQuote.append({"field":CONST_VISIBILITY_COUNT_QUOTE, "value":1, "type":"+"})
            updateFieldsReply.append({"field":CONST_VISIBILITY_VALUE, "value":value, "type":"+"})
            updateFieldsReply.append({"field":CONST_VISIBILITY_COUNT_REPLY, "value":1, "type":"+"})
            #update quoteId with updateFieldsQuote
            #update replyId with updateFieldsReply

        elif quoteId != None and replyId != None and quoteId == replyId:
            value = tweet[ProcessTweet.CONST_USER][ProcessTweet.CONST_FOLLOWERS_COUNT] * ProcessTweet.CONST_REPLY_VALUE
            updateFieldsQuote.append({"field":CONST_VISIBILITY_VALUE, "value":value, "type":"+"})
            updateFieldsQuote.append({"field":CONST_VISIBILITY_COUNT_QUOTE, "value":1, "type":"+"})
            updateFieldsQuote.append({"field":CONST_VISIBILITY_COUNT_REPLY, "value":1, "type":"+"})
            #update quoteId with updateFieldsQuote

        elif quoteId != None and replyId == None:
            value = tweet[ProcessTweet.CONST_USER][ProcessTweet.CONST_FOLLOWERS_COUNT] * ProcessTweet.CONST_QUOTE_VALUE
            updateFieldsQuote.append({"field":CONST_VISIBILITY_VALUE, "value":value, "type":"+"})
            updateFieldsQuote.append({"field":CONST_VISIBILITY_COUNT_QUOTE, "value":1, "type":"+"})
            #update quoteId with updateFieldsQuote

        #Crear metodo para solo actualizar clearTweet si no se ha contado ya la quote
        
        #Crear array con una o dos queries para actualiar (depende de lso if/else de antes)
        #Crear otra query para actualizar el tweet del quote (metodo processNormalTweet) 
        #Crear consulta de update en bulk (buscar como se hace)
       

    def processNormal (self, tweet, isReply):
        #Actualizar replyID si es reply
        #metodo processNormalTweet
        pass


    def getOriginalTweet(self, tweet):
        auxTweet = tweet
        while ProcessTweet.CONST_QUOTE in auxTweet:
            auxTweet = auxTweet[ProcessTweet.CONST_QUOTE]
        
        return auxTweet

    