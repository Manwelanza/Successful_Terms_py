class Neo4jDB():

    def __init__(self, connect2Neo4J):
        self.connect2Neo4J = connect2Neo4J

    def callDB (self, query):
        db = self.connect2Neo4J.getDB()
        if db != None:
            return db.run(query)
        else:
            return None

    def upsertTweet (self, tweetId, value):
        """
        query = "MERGE (t:Tweet {value: '" + tweetId + "'}) "
        query += "ON MATCH SET t.visibility = t.visibility + " + str(value) + " "
        query += "ON CREATE SET t = {value: '" + tweetId + "', visibility: 0} "
        """
        query =     "MERGE (t:Tweet {id:$id}) " 
        query +=    "ON MATCH SET t.visibility = t.visibility + $visivility " 
        query +=    "ON CREATE SET t = {id:$id, visibility:0}"
        
        db = self.connect2Neo4J.getDB()
        if db != None:
            return db.run(query, id=tweetId, visivility=value)
        else:
            return None

        