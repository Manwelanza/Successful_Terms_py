from credentials import *
from Neo4JDBConnect import *
from Neo4JDB import *

"""from neo4j.v1 import GraphDatabase


driver = GraphDatabase.driver(CONST_NEO4J_URI, auth=(CONST_NEO4J_USER, CONST_NEO4J_PASSWORD))

def add_friends(tx, name, friend_name):
    tx.run("MERGE (a:Person {name: $name}) "
           "MERGE (a)-[:KNOWS]->(friend:Person {name: $friend_name})",
           name=name, friend_name=friend_name)

def print_friends(tx, name):
    for record in tx.run("MATCH (a:Person)-[:KNOWS]->(friend) WHERE a.name = $name "
                         "RETURN friend.name ORDER BY friend.name", name=name):
        print(record["friend.name"])

with driver.session() as session:
    session.write_transaction(add_friends, "Arthur", "Guinevere")
    session.write_transaction(add_friends, "Arthur", "Lancelot")
    session.write_transaction(add_friends, "Arthur", "Merlin")
    session.read_transaction(print_friends, "Arthur")
"""

db = Neo4jDB (Connect2Neo4J (CONST_NEO4J_URI, CONST_NEO4J_USER, CONST_NEO4J_PASSWORD))
"""db.upsertTweet("1", 0)
db.upsertTweet("2", 0)
db.upsertTweet("3", 5)
db.upsertTweet("1", 2)
db.upsertTweet("1", 1)"""


db.insertTweet({"id":"1", "visibility":0, "level":0, "type":"Unknow", "parentId":None})

db.insertTweetAndRelation("1", "2", "QT")
db.insertTweetAndRelation("1", "3", "RP")
db.insertTweetAndRelation("1", "4", "RT")

db.insertTweetAndRelation("2", "5", "QT")
db.insertTweetAndRelation("2", "6", "RT")
db.insertTweetAndRelation("3", "7", "RT")

db.insertTweetAndRelation("5", "8", "QT")
db.insertTweetAndRelation("5", "9", "RP")
db.insertTweetAndRelation("5", "10", "RT")

"""
tweetId = "9"
data = list(db.searchTweet(tweetId).records())
data2Update = {}
if (len(data) > 0):
    source = data[0]["tweet"]
    level = source.get("level")
    value = 1 #calcular este valor en funcion del type se source
    data = list(db.getPath(tweetId).records())
    size = len(data)
    for i in range(size):
        node = data[i]["tweet"]
        if not node.get("id") in data2Update:
            data2Update[node.get("id")] = {"id":node.get("id"), "visibility":0}
       
        data2Update[node.get("id")]["visibility"] += (value / (level - node.get("level")))

db.bulkUpdate(list(data2Update.values()))
"""

db.insertTweet({"id":"0", "visibility":0, "level":0, "type":"normal", "parentId":None})
db.updateHead("0", "1", "RP")
db.insertRelation("0", "1")
db.addOneLevel("0")


tweetId = "0"
newValue = 0
data = list(db.getChilds(tweetId).records())
size = len(data)
for i in range(size):
    node = data[i]["tweet"]
    nodeType = node.get("type")
    print ("id: {0} -- level: {1} -- type: {2}".format(node.get("id"), node.get("level"), nodeType))

    if (nodeType != "RP"):
        newValue += (1 / node.get("level"))
    else:
        newValue += (0.5 / node.get("level"))

db.bulkUpdate([{"id":tweetId, "visibility":newValue}])

db.connect2Neo4J.closeDB()
"""
tweetId = "4"
data = list(db.searchTweet(tweetId).records())


if (len(data) > 0):
    print("Search")
    print(data[0].keys()[0])
    for n in data:
        print(n["tweet"].get("id"))
else:
    print("Insert")
    #db.insertTweet({"id":tweetId, "visibility":5})
    #db.insertRelation("1", tweetId, "RT")
    db.insertTweetAndRelation("1", {"id":tweetId, "visibility":5}, "RT")"""


"""
Ideas para el grafo:
    * Si es normalTweet consultar si existe y traer id_str y profundidad de su padre y de los padres de los padres
        * Si no existe insertar con profundidad 0 (siempre se mira primero la base)
        * Si existe 
            * Si no es reply, ni quote no hacer nada
            * Si es reply, perlo no quote actualizar padre en cadena hacia arriba
            * Si es reply y quote no hacer nada (ya se habra hecho en el procesamiento de quote)
    * Si es RT actualizar su padre y padre de su padre
    * Si es quote (sea reply o no) actualizar padre y padre de padres y llamar a normalTweet para almacenar el tweet 


CREATE INDEX ON :Tweet(id)
"""
