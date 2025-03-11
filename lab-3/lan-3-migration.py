import pandas as pd
from sqlalchemy import create_engine, Column, Integer, Float, String, Enum, Date, Time
from sqlalchemy.orm import sessionmaker, declarative_base
import enum
import psycopg2

pd.set_option('display.max_columns', None)     # показувати всі колонки
pd.set_option('display.max_rows', None)        # показувати всі рядки (тут на всяк випадок)
pd.set_option('display.max_colwidth', None)    # не обрізати значення в колонках
pd.set_option('display.expand_frame_repr', False)  # не переносити фрейм на новий рядок

# Крок 1: Читаємо CSV файл
df = pd.read_csv("GlobalWeatherRepository.csv")

# виведення списку хедерів, щоб визначити відповідно до категорії
list_of_column_names = list(df.columns)
print('Список хедерів : ',
      list_of_column_names)
print(df['wind_direction'].unique())

# Перевіряємо наявність необхідних колонок у CSV файлі
required_columns = ['country', 'wind_degree', 'wind_kph',
                    'wind_direction', 'last_updated', 'sunrise',
                    'precip_mm', 'precip_in']
#print(df[required_columns].head(10))

for col in required_columns:
    if col not in df.columns:
        print(f"Відсутня колонка: {col}")
        exit(1)

# Дані для підключення та створення БД
DB_NAME = "globalweather"
DB_USER = "postgres"
DB_PASSWORD = "adopted29v"
DB_HOST = "localhost"
DB_PORT = "5432"

# Ствоерення БД
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
class WeatherData(Base):
    __tablename__ = 'weather_data'

    id = Column(Integer, primary_key=True, autoincrement=True)
    country = Column(String, nullable=False)
    wind_degree = Column(Integer, nullable=False)
    wind_kph = Column(Float, nullable=False)
    wind_direction = Column(Enum(WindDirectionEnum), nullable=False)
    last_updated = Column(Date, nullable=False)
    sunrise = Column(Time, nullable=False)
    precip_mm = Column(Float, nullable=True)
    precip_in = Column(Float, nullable=True)

# Створюємо SQLAlchemy engine до нової бази
engine = create_engine(f'postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')

# Створення таблиці
Base.metadata.create_all(engine)

# Ініціалізуємо сесію
Session = sessionmaker(bind=engine)
session = Session()

# Перетворюємо дані з DataFrame в об'єкти ORM та додаємо їх до бази даних
for index, row in df.iterrows():
    new_record = WeatherData(
        country=row['country'],
        wind_degree=row['wind_degree'],
        wind_kph=row['wind_kph'],
        wind_direction=WindDirectionEnum(row['wind_direction']),
        last_updated=pd.to_datetime(row['last_updated']).date(),
        sunrise=pd.to_datetime(row['sunrise']).time(),
        precip_mm=row['precip_mm'],
        precip_in=row['precip_in']
    )
    session.add(new_record)

# Зберігаємо зміни в базі даних
session.commit()
session.close()
