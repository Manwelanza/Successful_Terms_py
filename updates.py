def updateClearTweets (tweet, updateFields):
    if tweet is None or updateFields is None or len(updateFields) <= 0:
        return None
    
    result = {}
    for field in updateFields:
        result[field] = tweet[field]
    
    return result

def updateClearTweetsV2 (updateFields):
    if updateFields is None or len(updateFields) <= 0:
        return None

    result = {}
    incValues = {}
    setValues = {}
    for field in updateFields:
        if field["type"] == "+":
            incValues[field["field"]] = field["value"]
        else:
            setValues[field["field"]] = field["value"]
    

    #TODO: if dictionary is empty is not working right
    
    if not incValues:
        result["$inc"] = incValues

    if not setValues:
        result["$set"] = setValues

    return result