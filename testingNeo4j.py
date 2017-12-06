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
