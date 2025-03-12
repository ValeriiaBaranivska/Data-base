from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from lab3migration import Base, Country, PrecipNum, WeatherData

# Налаштування підключення до PostgreSQL та MySQL
PG_ENGINE = create_engine('postgresql+psycopg2://postgres:adopted29v@localhost:5432/globalweather')
MYSQL_ENGINE = create_engine('mysql+pymysql://root:adopted29v@localhost:3306/globalweather')

def create_mysql_database():
    # Створення бази даних, якщо вона не існує
    with MYSQL_ENGINE.connect() as connection:
        connection.execute("CREATE DATABASE IF NOT EXISTS globalweather")

    # Підключення до створеної бази даних
    return create_engine('mysql+pymysql://root:adopted29v@localhost:3306/globalweather')

def migrate_data():
    # Створення бази даних MySQL, якщо вона не існує
    mysql_engine = create_mysql_database()

    # Створення таблиць у MySQL
    Base.metadata.create_all(mysql_engine)

    # Ініціалізація сесій
    PGSession = sessionmaker(bind=PG_ENGINE)
    MYSQLSession = sessionmaker(bind=mysql_engine)

    pg_session = PGSession()
    mysql_session = MYSQLSession()

    try:
        # Перенесення даних з PostgreSQL до MySQL
        for country in pg_session.query(Country).all():
            mysql_session.merge(country)

        for precip in pg_session.query(PrecipNum).all():
            mysql_session.merge(precip)

        for weather_data in pg_session.query(WeatherData).all():
            mysql_session.merge(weather_data)

        # Зберігаємо зміни в MySQL
        mysql_session.commit()
    except Exception as e:
        print(f"Помилка міграції: {e}")
    finally:
        mysql_session.close()
        pg_session.close()

if __name__ == "__main__":
    migrate_data()