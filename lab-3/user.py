from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, joinedload
from datetime import datetime
from lab3migration import Country, PrecipNum, WeatherData

# Database connection details
DB_NAME = "globalweather"
DB_USER = "postgres"
DB_PASSWORD = "adopted29v"
DB_HOST = "localhost"
DB_PORT = "5432"

# Create SQLAlchemy engine
engine = create_engine(f'postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')
Session = sessionmaker(bind=engine)
session = Session()

# Input from user
country = input("Введіть країну: ").strip()
date_input = input("Введіть дату (рррр-мм-дд): ").strip()
precip = float(input("Введіть рівень опадів (у мм): ").strip())

try:
    date_obj = datetime.strptime(date_input, "%Y-%m-%d").date()
except ValueError:
    print("Неправильний формат дати. Використовуйте РРРР-ММ-ДД.")
    exit()

# Optimized query with joinedload for eager loading
results = (
    session.query(WeatherData)
    .options(joinedload(WeatherData.country), joinedload(WeatherData.precip))
    .join(Country, WeatherData.country_id == Country.id)
    .join(PrecipNum, WeatherData.precip_id == PrecipNum.id)
    .filter(
        Country.name.ilike(f"%{country}%"),
        WeatherData.last_updated == date_obj,
        PrecipNum.precip_mm == precip
    )
    .all()
)

if not results:
    print("Даних не знайдено.")
else:
    print("\nЗнайдено запис(и):")
    for row in results:
        print(f"""
          Країна: {row.country.name}
          Дата останнього оновленя: {row.last_updated}
          Світанок: {row.sunrise}
          Ступінь вітру: {row.wind_degree}°
          Швидкість вітру: {row.wind_kph} км/год
          Напрям вітру: {row.wind_direction}
          Кількість опадів: {row.precip.precip_mm} мм
          Кількість опадів: {row.precip.precip_in} in
          Чи можна йти на вулицю: {row.precip.precip_bool}
          """)

session.close()
