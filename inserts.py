CONST_TWEET_ID = "tweetId"
CONST_IS_QUOTE = "isQuote"
CONST_IS_REPLY = "isReply"
CONST_REPLY_TO = "replyTo"
CONST_QUOTE_TO = "quoteTo"
CONST_IS_LONG = "isLong"
CONST_CHARACTERS = "characters"
CONST_MENTIONS = "mentions"
CONST_TERMS_COUNT = "terms_count"


def getInsertClearTweet (tweet, isQuote, isReply, processTweet):
    if tweet is None:
        return None

    clearTweet = {}
    clearTweet[CONST_TWEET_ID] = tweet[processTweet.CONST_ID]
    
    if isQuote is True:
        clearTweet[CONST_IS_QUOTE] = True
        if processTweet.CONST_QUOTE_TO_STATUS_ID in tweet:
            clearTweet[CONST_QUOTE_TO] = tweet[processTweet.CONST_QUOTE_TO_STATUS_ID]
        else:
            clearTweet[CONST_QUOTE_TO] = None
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
    
    if processTweet.CONST_DISPLAY_TEXT_RANGE in tweet and processTweet.CONST_DISPLAY_TEXT_RANGE in tweet[processTweet.CONST_EXTENDED_TWEET]:
        clearTweet[CONST_CHARACTERS] = tweet[processTweet.CONST_DISPLAY_TEXT_RANGE][1] + tweet[processTweet.CONST_EXTENDED_TWEET][processTweet.CONST_DISPLAY_TEXT_RANGE][1]
    else:
        clearTweet[CONST_CHARACTERS] = -1
    
    
    clearTweet[processTweet.CONST_HASHTAGS] = tweet[processTweet.CONST_EXTENDED_TWEET][processTweet.CONST_ENTITIES][processTweet.CONST_HASHTAGS]
    clearTweet[CONST_MENTIONS] = tweet[processTweet.CONST_EXTENDED_TWEET][processTweet.CONST_ENTITIES][processTweet.CONST_USER_MENTIONS]
    clearTweet[processTweet.CONST_SYMBOLS] = tweet[processTweet.CONST_EXTENDED_TWEET][processTweet.CONST_ENTITIES][processTweet.CONST_SYMBOLS]
    
    if processTweet.CONST_MEDIA in tweet[processTweet.CONST_EXTENDED_TWEET][processTweet.CONST_ENTITIES]:
        clearTweet[processTweet.CONST_MEDIA] = tweet[processTweet.CONST_EXTENDED_TWEET][processTweet.CONST_ENTITIES][processTweet.CONST_MEDIA]
    else:
        clearTweet[processTweet.CONST_MEDIA] = None
    
    if processTweet.CONST_URLS in tweet[processTweet.CONST_EXTENDED_TWEET][processTweet.CONST_ENTITIES]:
        clearTweet[processTweet.CONST_URLS] = tweet[processTweet.CONST_EXTENDED_TWEET][processTweet.CONST_ENTITIES][processTweet.CONST_URLS]
    else:
        clearTweet[processTweet.CONST_URLS] = None

    return clearTweet

def loadNoTruncatedData (tweet, clearTweet, processTweet):
    text = tweet[processTweet.CONST_TEXT]
    clearTweet[processTweet.CONST_TEXT] = text
    clearTweet[CONST_TERMS_COUNT] = len(text.split())
    clearTweet[CONST_IS_LONG] = False

    if processTweet.CONST_DISPLAY_TEXT_RANGE in tweet:
        clearTweet[CONST_CHARACTERS] = tweet[processTweet.CONST_DISPLAY_TEXT_RANGE][1]
    else:
        clearTweet[CONST_CHARACTERS] = -1

    clearTweet[processTweet.CONST_HASHTAGS] = tweet[processTweet.CONST_ENTITIES][processTweet.CONST_HASHTAGS]
    clearTweet[CONST_MENTIONS] = tweet[processTweet.CONST_ENTITIES][processTweet.CONST_USER_MENTIONS]
    clearTweet[processTweet.CONST_SYMBOLS] = tweet[processTweet.CONST_ENTITIES][processTweet.CONST_SYMBOLS]
    
    if processTweet.CONST_MEDIA in tweet[processTweet.CONST_ENTITIES]:
        clearTweet[processTweet.CONST_MEDIA] = tweet[processTweet.CONST_ENTITIES][processTweet.CONST_MEDIA]
    else:
        clearTweet[processTweet.CONST_MEDIA] = None

    if processTweet.CONST_URLS in tweet[processTweet.CONST_ENTITIES]:
        clearTweet[processTweet.CONST_URLS] = tweet[processTweet.CONST_ENTITIES][processTweet.CONST_URLS]
    else:
        clearTweet[processTweet.CONST_URLS] = None

    return clearTweet