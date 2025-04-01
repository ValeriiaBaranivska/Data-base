from conect_to_neo4j import execute_query

# Створення вузлів та зв'язків

create_nodes_query = """
CREATE (i1:Items {id: 1, name: 'Laptop', price: 1000})
CREATE (i2:Items {id: 2, name: 'Smartphone', price: 500})
CREATE (c1:Customer {id: 1, name: 'Valeriia'})
CREATE (c2:Customer {id: 2, name: 't'})
CREATE (o1:Orders {id: 1, date: '2023-10-01'})
CREATE (o2:Orders {id: 2, date: '2023-10-02'})
"""

create_relationships_query = """
CREATE (c1)-[:BOUGHT]->(o1)
CREATE (c2)-[:BOUGHT]->(o2)
CREATE (o1)-[:CONTAINS]->(i1)
CREATE (o2)-[:CONTAINS]->(i2)
CREATE (c1)-[:VIEWED]->(i2)
CREATE (c2)-[:VIEWED]->(i1)
"""

# Виконання запитів для створення вузлів та зв'язків
execute_query(create_nodes_query)
execute_query(create_relationships_query)

print("Схема створена успішно!")