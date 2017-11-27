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

def getInsertClearTweet (tweet, isQuote, isReply, processTweet):
    if tweet is None:
        return None

    clearTweet = {}
    clearTweet[CONST_TWEET_ID] = tweet[processTweet.CONST_ID]
    
    if isQuote is True:
        clearTweet[CONST_IS_QUOTE] = True
        clearTweet[CONST_QUOTE_TO] = tweet[processTweet.CONST_QUOTE_TO_STATUS_ID]
    else:
        clearTweet[CONST_IS_QUOTE] = False
        clearTweet[CONST_QUOTE_TO] = None

    if isReply is True:
        clearTweet[CONST_IS_REPLY] = True
        clearTweet[CONST_REPLY_TO] = tweet[processTweet.CONST_REPLY_TO_STATUS_ID]
    else:
        clearTweet[CONST_IS_REPLY] = False
        clearTweet[CONST_REPLY_TO] = None

    if tweet[processTweet.CONST_TRUNCATED]:
        clearTweet = loadTruncatedData (tweet, clearTweet, processTweet)
    else:
        clearTweet = loadNoTruncatedData (tweet, clearTweet, processTweet)

    clearTweet[processTweet.CONST_REPLY_COUNT] = tweet[processTweet.CONST_REPLY_COUNT]
    clearTweet[processTweet.CONST_RT_COUNT] = tweet[processTweet.CONST_RT_COUNT]
    clearTweet[processTweet.CONST_QUOTE_COUNT] = tweet[processTweet.CONST_QUOTE_COUNT]
    clearTweet[processTweet.CONST_FAVORITE_COUNT] = tweet[processTweet.CONST_FAVORITE_COUNT]
    clearTweet[processTweet.CONST_LANGUAGE] = tweet[processTweet.CONST_LANGUAGE]
    clearTweet[processTweet.CONST_COORDINATES] = tweet[processTweet.CONST_COORDINATES]
    clearTweet[processTweet.CONST_USER] = tweet[processTweet.CONST_USER]
    clearTweet[processTweet.CONST_CREATED_AT] = tweet[processTweet.CONST_CREATED_AT]

    return clearTweet

def loadTruncatedData (tweet, clearTweet, processTweet):
    text = tweet[processTweet.CONST_EXTENDED_TWEET][processTweet.CONST_FULL_TEXT]
    clearTweet[processTweet.CONST_TEXT] = text
    clearTweet[CONST_TERMS_COUNT] = len(text.split())
    clearTweet[CONST_IS_LONG] = True
    clearTweet[CONST_CHARACTERS] = tweet[processTweet.CONST_DISPLAY_TEXT_RANGE][1] + tweet[processTweet.CONST_EXTENDED_TWEET][processTweet.CONST_DISPLAY_TEXT_RANGE][1]
    clearTweet[processTweet.CONST_HASHTAGS] = tweet[processTweet.CONST_EXTENDED_TWEET][processTweet.CONST_ENTITIES][processTweet.CONST_HASHTAGS]
    clearTweet[CONST_MENTIONS] = tweet[processTweet.CONST_EXTENDED_TWEET][processTweet.CONST_ENTITIES][processTweet.CONST_USER_MENTIONS]
    clearTweet[processTweet.CONST_SYMBOLS] = tweet[processTweet.CONST_EXTENDED_TWEET][processTweet.CONST_ENTITIES][processTweet.CONST_SYMBOLS]
    clearTweet[processTweet.CONST_MEDIA] = tweet[processTweet.CONST_EXTENDED_TWEET][processTweet.CONST_ENTITIES][processTweet.CONST_MEDIA]
    return clearTweet

def loadNoTruncatedData (tweet, clearTweet, processTweet):
    text = tweet[processTweet.CONST_TEXT]
    clearTweet[processTweet.CONST_TEXT] = text
    clearTweet[CONST_TERMS_COUNT] = len(text.split())
    clearTweet[CONST_IS_LONG] = False
    clearTweet[CONST_CHARACTERS] = tweet[processTweet.CONST_DISPLAY_TEXT_RANGE][1]
    clearTweet[processTweet.CONST_HASHTAGS] = tweet[processTweet.CONST_ENTITIES][processTweet.CONST_HASHTAGS]
    clearTweet[CONST_MENTIONS] = tweet[processTweet.CONST_ENTITIES][processTweet.CONST_USER_MENTIONS]
    clearTweet[processTweet.CONST_SYMBOLS] = tweet[processTweet.CONST_ENTITIES][processTweet.CONST_SYMBOLS]
    clearTweet[processTweet.CONST_MEDIA] = tweet[processTweet.CONST_ENTITIES][processTweet.CONST_MEDIA]
    return clearTweet