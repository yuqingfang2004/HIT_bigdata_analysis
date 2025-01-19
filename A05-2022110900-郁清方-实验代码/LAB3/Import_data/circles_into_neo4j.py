from py2neo import Graph

# 连接到 Neo4j 数据库
graph = Graph("bolt://localhost:7687", auth=("neo4j", "40027070"))

# 执行 Cypher 插入
query = """
LOAD CSV WITH HEADERS FROM 'file:///csv_data/circles.csv' AS row
MERGE (c:Circle {name: row.circle_name})
WITH c, row
MATCH (p:Person {id: row.member_id})
MERGE (p)-[:BELONGS_TO]->(c);
"""
graph.run(query)