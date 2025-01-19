from py2neo import Graph

# 连接到 Neo4j 数据库
graph = Graph("bolt://localhost:7687", auth=("neo4j", "40027070"))

# 执行 Cypher 插入
query = """
LOAD CSV WITH HEADERS FROM 'file:///csv_data/features.csv' AS row
MERGE (p:Person {id: row.node_id})
WITH p, row
UNWIND keys(row) AS key
WITH p, key, row[key] AS value
WHERE key <> 'node_id' AND value <> ''
SET p[key] = value;
"""
graph.run(query)