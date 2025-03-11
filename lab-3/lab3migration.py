import pandas as pd
from sqlalchemy import ForeignKey, create_engine, Column, Integer, Float, String, Enum, Date, Time
from sqlalchemy.orm import sessionmaker, declarative_base
import enum
import psycopg2
import alembic

# Крок 1: Читаємо CSV файл
df = pd.read_csv("GlobalWeatherRepository.csv")

# виведення списку хедерів, щоб визначити відповідно до категорії
list_of_column_names = list(df.columns)
#print('Список хедерів : ',
#      list_of_column_names)
#print(df['wind_direction'].unique())

# Перевіряємо наявність необхідних колонок у CSV файлі
required_columns = ['country', 'wind_degree', 'wind_kph',
                    'wind_direction', 'last_updated', 'sunrise',
                    'precip_mm', 'precip_in']
#print(df[required_columns].head(10))

# Дані для підключення та створення БД
DB_NAME = "globalweather"
DB_USER = "postgres"
DB_PASSWORD = "adopted29v"
DB_HOST = "localhost"
DB_PORT = "5432"

# Створення БД
conn = psycopg2.connect(
   database="postgres", user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT
)
conn.autocommit = True
cursor = conn.cursor()

try:
    cursor.execute(f'CREATE DATABASE {DB_NAME}')
    print(f"Database '{DB_NAME}' created successfully.")
except psycopg2.errors.DuplicateDatabase:
    print(f"Database '{DB_NAME}' already exists.")
finally:
    cursor.close()
    conn.close()

# Створюємо базовий клас для ORM
Base = declarative_base()

# Визначаємо перелік для напрямку вітру
class WindDirectionEnum(enum.Enum):
    NNW = 'NNW'
    NW = 'NW'
    W = 'W'
    SW = 'SW'
    SSE = 'SSE'
    E = 'E'
    N = 'N'
    SE = 'SE'
    ESE = 'ESE'
    NNE ='NNE'
    S = 'S'
    WSW = 'WSW'
    SSW ='SSW'
    ENE = 'ENE'
    NE = 'NE'
    WNW = 'WNW'

# опис ORM
class Country(Base):
    __tablename__ = 'country'
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, nullable=False)

class PrecipNum(Base):
    __tablename__ = 'precipitate'
    id = Column(Integer, primary_key=True, autoincrement=True)
    precip_mm = Column(Float, nullable=True)
    precip_in = Column(Float, nullable=True)

class WeatherData(Base):
    __tablename__ = 'weather_data'

    id = Column(Integer, primary_key=True, autoincrement=True)
    wind_degree = Column(Integer, nullable=False)
    wind_kph = Column(Float, nullable=False)
    wind_direction = Column(Enum(WindDirectionEnum), nullable=False)
    last_updated = Column(Date, nullable=False)
    sunrise = Column(Time, nullable=False)

    country_id = Column(Integer, ForeignKey('country.id'), nullable=False)
    precip_mm_id = Column(Integer, ForeignKey('precipitate.id'))
    precip_in_id = Column(Integer, ForeignKey('precipitate.id'))
# Створюємо SQLAlchemy engine до нової бази
engine = create_engine(f'postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')

# Створення таблиці
#WeatherData.__table__.drop(engine)
Base.metadata.create_all(engine)

# Ініціалізуємо сесію
Session = sessionmaker(bind=engine)
session = Session()

# Перетворюємо дані з DataFrame в об'єкти ORM та додаємо їх до бази даних
for index, row in df.iterrows():
    country = session.query(Country).filter_by(name=row['country']).first()
    if not country:
        country = Country(name=row['country'])
        session.add(country)
        session.commit()

    precip_mm_num = session.query(PrecipNum).filter_by(precip_mm=row['precip_mm']).first()
    precip_in_num = session.query(PrecipNum).filter_by(precip_in=row['precip_in']).first()
    if not precip_mm_num:
        precip_mm_num = PrecipNum(precip_mm=row['precip_mm'])
        session.add(precip_mm_num)

    if not precip_in_num:
        precip_in_num = PrecipNum(precip_in=row['precip_in'])
        session.add(precip_in_num)

    # Додавання погодних даних
    weather_data = WeatherData(
        wind_degree=row['wind_degree'],
        wind_kph=row['wind_kph'],
        wind_direction=row['wind_direction'],
        last_updated=pd.to_datetime(row['last_updated']).date(),
        sunrise=pd.to_datetime(row['sunrise']).time(),

        country_id=country.id,
        precip_mm_id = precip_mm_num.id,
        precip_in_id = precip_in_num.id
    )
    session.add(weather_data)

# Зберігаємо зміни в базі даних
session.commit()
session.close()
