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
db.upsertTweet("1", 0)
db.upsertTweet("2", 0)
db.upsertTweet("3", 5)
db.upsertTweet("1", 2)
db.upsertTweet("1", 1)


#print(db.searchTweet("2").values())

#print(list(db.searchTweet("2").records())[0].values()[0])
#print(list(db.searchTweet("2").records())[0].values())
tweetId = "4"
data = list(db.searchTweet(tweetId).records())


if (len(data) > 0):
    print("Search")
    print(data[0].keys()[0])
    for n in data:
        print(n["t"].get("id"))
else:
    print("Insert")
    #db.insertTweet({"id":tweetId, "visibility":5})
    #db.insertRelation("1", tweetId, "RT")
    db.insertTweetAndRelation("1", {"id":tweetId, "visibility":5}, "RT")


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

"""
