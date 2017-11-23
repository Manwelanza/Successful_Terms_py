def getRTs ():
    return {"retweeted_status": { "$exists" : True}}

def getQuotes ():
    return {"quoted_status": { "$exists" : True}}

def getNormalTweets():
    return ({
            "$and": [
                        {"quoted_status": { "$exists" : False}},
                        {"retweeted_status": { "$exists" : False}},
                        {"in_reply_to_status_id_str": None}
                    ]
            })

def getReplys ():
    return {"in_reply_to_status_id_str": {"$ne": None}}

def getTweet (idLabel, idValue):
    return {idLabel: idValue}




