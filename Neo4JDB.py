class Neo4jDB():

    def __init__(self, connect2Neo4J):
        self.connect2Neo4J = connect2Neo4J

    def callDB (self, query):
        db = self.connect2Neo4J.getDB()
        if db != None:
            return db.run(query)
        else:
            return None


    def upsert (self, parentId, tweetId):
        query = "MERGE (t:Tweet {id:$id}) ON CREATE SET t = {id:$id, parentId:$parent, visibility:0}"
        db = self.connect2Neo4J.getDB()
        if db != None:
            return db.run(query, id=tweetId, parent=parentId)
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

    def upsertTypeAndRelation (self,parentId, tweetId, reltype):
        query = """
            MATCH (t2: Tweet {id:$parent})
            USING INDEX t2:Tweet(id) 
            MERGE (t:Tweet {id:$id})
            ON CREATE SET t = {id:$id, parentId:$parent, visibility:0}
            MERGE (t2)<-[r:RELTYPE]-(t)
            ON CREATE SET r = {type:$relType}
            ON MATCH SET r = {type:$relType}
        """

        db = self.connect2Neo4J.getDB()
        if db != None:
            return db.run(query, id=tweetId, parent = parentId, relType = reltype)
        else:
            return None


    def searchTweet (self, tweetId):
        query = "MATCH (t:Tweet {id:$id}) RETURN t as tweet"
        db = self.connect2Neo4J.getDB()
        if db != None:
            return db.run(query, id=tweetId)
        else:
            return None

    def getPath (self, sourceId):
        query = "MATCH (t1:Tweet {id:$id})-[r:RELTYPE *..]->(t2:Tweet) RETURN t2 as tweet ORDER BY t2.level DESC"
        db = self.connect2Neo4J.getDB()
        if db != None:
            return db.run(query, id=sourceId)
        else:
            return None

    def getChilds (self, sourceId):
        query = "MATCH (t1:Tweet {id:$id})<-[r:RELTYPE *..]-(t2:Tweet) RETURN t2 as tweet ORDER BY t2.level ASC"
        db = self.connect2Neo4J.getDB()
        if db != None:
            return db.run(query, id=sourceId)
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

    def updateHead (self, parentId, tweetId, relType):
        query = """
        MATCH (t:Tweet {id:$id})
        USING INDEX t:Tweet(id) 
        SET t.type = $relType, t.level = 0, t.parentId = $parentId
        """
        db = self.connect2Neo4J.getDB()
        if db != None:
            db.run(query, id=tweetId, relType=relType, parentId=parentId)
            return True
        else:
            return False

    def insertRelation (self, parentId, childId):
        query = "MATCH (t1:Tweet {id:$id1}), (t2:Tweet {id:$id2}) CREATE (t1)-[r:RELTYPE]->(t2)"
        db = self.connect2Neo4J.getDB()
        if db != None:
            db.run(query, id1=childId, id2=parentId)
            return True
        else:
            return False
    
    def insertTweetAndRelation (self, parentId, childId, relType):
        query = "MATCH (t1:Tweet {id:$parentId}) CREATE (t2:Tweet {id:$childId, visibility:0, parentId:t1.id})-[r:RELTYPE {type:$relType}]->(t1)"
        db = self.connect2Neo4J.getDB()
        if db != None:
            db.run(query, parentId=parentId, childId=childId, relType=relType)
            return True
        else:
            return False

    def bulkUpdate (self, dataList):
        query = """
            UNWIND {rows} as row
            MATCH (t:Tweet {id:row.id}) 
            USING INDEX t:Tweet(id) 
            SET t.visibility = t.visibility + row.visibility
            """
        db = self.connect2Neo4J.getDB()
        if db != None:
            db.run(query, rows=dataList)
            return True
        else:
            return False
    
    def addOneLevel (self, sourceId):
        query = """
            MATCH (t:Tweet {id:$id})<-[r:RELTYPE *..]-(t2:Tweet)
            USING INDEX t:Tweet(id) 
            SET t2.level = t2.level + 1
        """
        db = self.connect2Neo4J.getDB()
        if db != None:
            db.run(query, id=sourceId)
            return True
        else:
            return False
            