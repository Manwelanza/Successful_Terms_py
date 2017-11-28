def updateClearTweets (tweet, updateFields):
    if tweet is None or updateFields is None or len(updateFields) <= 0:
        return None
    
    result = {}
    for field in updateFields:
        result[field] = tweet[field]
    
    return result