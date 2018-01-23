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

    def searchTweet (self, tweetId):
        query = "MATCH (t:Tweet {id:$id}) RETURN t"
        db = self.connect2Neo4J.getDB()
        if db != None:
            return db.run(query, id=tweetId)
        else:
            return None

    def insertTweet (self, dataToInsert):
        query = "CREATE (t:Tweet $data)"
        db = self.connect2Neo4J.getDB()
        if db != None:
            db.run(query, data=dataToInsert)
            return True
        else:
            return False

    def insertRelation (self, parentId, childId, relationType):
        query = "MATCH (t1:Tweet {id:$id1}), (t2:Tweet {id:$id2}) CREATE (t1)-[r:RELTYPE {type:$relType}]->(t2)"
        db = self.connect2Neo4J.getDB()
        if db != None:
            db.run(query, id1=childId, id2=parentId, relType=relationType)
            return True
        else:
            return False
    
    def insertTweetAndRelation (self, parentId, dataToInsert, relationType):
        query = "MATCH (t1:Tweet {id:$id}) CREATE (t2:Tweet $data)-[r:RELTYPE {type:$relType}]->(t1)"
        db = self.connect2Neo4J.getDB()
        if db != None:
            db.run(query, id=parentId, data=dataToInsert, relType=relationType)
            return True
        else:
            return False