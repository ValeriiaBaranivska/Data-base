from neo4j import GraphDatabase

# URI examples: "neo4j://localhost", "neo4j+s://xxx.databases.neo4j.io"
URI = "neo4j+s://75562f58.databases.neo4j.io"
AUTH = ("neo4j", "phT8NyoRRynYVlRxZVJgDGyYraQnDlCG5Mu2JC2MGRo")


# Встановлення з'єднання з базою даних
with GraphDatabase.driver(URI, auth=AUTH) as driver:
    # Функція для виконання запиту
    def execute_query(query):
        with driver.session() as session:
            result = session.run(query)
            return [record for record in result]

    # Перевірка підключення
    try:
        query = "MATCH (n) RETURN n LIMIT 1"
        results = execute_query(query)
        print("Підключення вдале")
        for record in results:
            print(record)
    except Exception as e:
        print("Помилка підключення:", e)



