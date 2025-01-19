from py2neo import Graph

# 连接到 Neo4j 数据库
graph = Graph("bolt://localhost:7687", auth=("neo4j", "40027070"))

# 执行 Cypher 插入
query = """
LOAD CSV WITH HEADERS FROM 'file:///csv_data/edges.csv' AS row
MATCH (a:Person {id: row.source})
MATCH (b:Person {id: row.target})
MERGE (a)-[:Be_Friend_With]->(b);
"""
graph.run(query)