import processTweet

CONST_TWEET_ID = "tweetId"
CONST_IS_QUOTE = "isQuote"
CONST_IS_REPLY = "isReply"
CONST_REPLY_TO = "replyTo"
CONST_QUOTE_TO = "quoteTo"
CONST_IS_LONG = "isLong"
CONST_CHARACTERS = "characters"
CONST_MENTIONS = "mentions"
CONST_TERMS_COUNT = "terms_count"


def getInsertNewClearTweet (tweetId):
    return ({
                "tweetId" : tweetId,
                "rts": 0,
                "quotes": 0,
                "replys": 0
            })

def getInsertClearTweet (tweet, isQuote, isReply):
    if tweet is None:
        return None

    clearTweet = {}
    clearTweet[CONST_TWEET_ID] = tweet[ProcessTweet.CONST_ID]
    
    if isQuote is True:
        clearTweet[CONST_IS_QUOTE] = True
        clearTweet[CONST_QUOTE_TO] = tweet[ProcessTweet.CONST_QUOTE_TO_STATUS_ID]
    else:
        clearTweet[CONST_IS_QUOTE] = False
        clearTweet[CONST_QUOTE_TO] = None

    if isReply is True:
        clearTweet[CONST_IS_REPLY] = True
        clearTweet[CONST_REPLY_TO] = tweet[ProcessTweet.CONST_REPLY_TO_STATUS_ID]
    else:
        clearTweet[CONST_IS_REPLY] = False
        clearTweet[CONST_REPLY_TO] = None

    if tweet[ProcessTweet.CONST_TRUNCATED]:
        clearTweet = loadTruncatedData (tweet, clearTweet)
    else:
        clearTweet = loadNoTruncatedData (tweet, clearTweet)

    clearTweet[ProcessTweet.CONST_REPLY_COUNT] = tweet[ProcessTweet.CONST_REPLY_COUNT]
    clearTweet[ProcessTweet.CONST_RT_COUNT] = tweet[ProcessTweet.CONST_RT_COUNT]
    clearTweet[ProcessTweet.CONST_QUOTE_COUNT] = tweet[ProcessTweet.CONST_QUOTE_COUNT]
    clearTweet[ProcessTweet.CONST_FAVORITE_COUNT] = tweet[ProcessTweet.CONST_FAVORITE_COUNT]
    clearTweet[ProcessTweet.CONST_LANGUAGE] = tweet[ProcessTweet.CONST_LANGUAGE]
    clearTweet[ProcessTweet.CONST_COORDINATES] = tweet[ProcessTweet.CONST_COORDINATES]
    clearTweet[ProcessTweet.CONST_USER] = tweet[ProcessTweet.CONST_USER]
    clearTweet[ProcessTweet.CONST_CREATED_AT] = tweet[ProcessTweet.CONST_CREATED_AT]

    return clearTweet

def loadTruncatedData (tweet, clearTweet):
    text = tweet[ProcessTweet.CONST_EXTENDED_TWEET][ProcessTweet.CONST_FULL_TEXT]
    clearTweet[ProcessTweet.CONST_TEXT] = text
    clearTweet[CONST_TERMS_COUNT] = len(text.split())
    clearTweet[CONST_IS_LONG] = True
    clearTweet[CONST_CHARACTERS] = tweet[ProcessTweet.CONST_DISPLAY_TEXT_RANGE][1] + tweet[ProcessTweet.CONST_EXTENDED_TWEET][ProcessTweet.CONST_DISPLAY_TEXT_RANGE][1]
    clearTweet[ProcessTweet.CONST_HASHTAGS] = tweet[ProcessTweet.CONST_EXTENDED_TWEET][ProcessTweet.CONST_ENTITIES][ProcessTweet.CONST_HASHTAGS]
    clearTweet[CONST_MENTIONS] = tweet[ProcessTweet.CONST_EXTENDED_TWEET][ProcessTweet.CONST_ENTITIES][ProcessTweet.CONST_USER_MENTIONS]
    clearTweet[ProcessTweet.CONST_SYMBOLS] = tweet[ProcessTweet.CONST_EXTENDED_TWEET][ProcessTweet.CONST_ENTITIES][ProcessTweet.CONST_SYMBOLS]
    clearTweet[ProcessTweet.CONST_MEDIA] = tweet[ProcessTweet.CONST_EXTENDED_TWEET][ProcessTweet.CONST_ENTITIES][ProcessTweet.CONST_MEDIA]
    return clearTweet

def loadNoTruncatedData (tweet, clearTweet):
    text = tweet[ProcessTweet.CONST_TEXT]
    clearTweet[ProcessTweet.CONST_TEXT] = text
    clearTweet[CONST_TERMS_COUNT] = len(text.split())
    clearTweet[CONST_IS_LONG] = False
    clearTweet[CONST_CHARACTERS] = tweet[ProcessTweet.CONST_DISPLAY_TEXT_RANGE][1]
    clearTweet[ProcessTweet.CONST_HASHTAGS] = tweet[ProcessTweet.CONST_ENTITIES][ProcessTweet.CONST_HASHTAGS]
    clearTweet[CONST_MENTIONS] = tweet[ProcessTweet.CONST_ENTITIES][ProcessTweet.CONST_USER_MENTIONS]
    clearTweet[ProcessTweet.CONST_SYMBOLS] = tweet[ProcessTweet.CONST_ENTITIES][ProcessTweet.CONST_SYMBOLS]
    clearTweet[ProcessTweet.CONST_MEDIA] = tweet[ProcessTweet.CONST_ENTITIES][ProcessTweet.CONST_MEDIA]
    return clearTweet