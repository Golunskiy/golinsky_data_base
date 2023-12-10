from sqlalchemy import create_engine, inspect
from sqlalchemy.orm import sessionmaker
from pymongo import MongoClient
from sqlalchemy.ext.automap import automap_base


# Підключення до PostgreSQL
POSTGRES_USER = "postgres"
POSTGRES_PASSWORD = "postgres"
POSTGRES_HOST = "localhost"
POSTGRES_DBNAME = "DB2"
postgres_engine = create_engine(f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}/{POSTGRES_DBNAME}")
PostgresSession = sessionmaker(bind=postgres_engine)
postgres_session = PostgresSession()

# Підключення до MongoDB
MONGO_USER = "golinsky"
MONGO_PASSWORD = "1234567890"
MONGO_CLUSTER = "cluster0.udq9qzo.mongodb.net"
MONGO_DBNAME = "DB_mongo"
mongo_client = MongoClient(f"mongodb+srv://{MONGO_USER}:{MONGO_PASSWORD}@{MONGO_CLUSTER}/{MONGO_DBNAME}")
mongo_db = mongo_client[MONGO_DBNAME]


Base = automap_base()
Base.prepare(postgres_engine, reflect=True)
inspector = inspect(postgres_engine)

# Models
Result = Base.classes.result
PT = Base.classes.pt
Reg = Base.classes.reg
Info = Base.classes.info
EO = Base.classes.eo
MainData2019 = Base.classes.maindata2019

models = [Result, PT, Reg, Info, EO, MainData2019]

# Datas
datas = []
for model in models:
    data = postgres_session.query(model).all()
    datas.append(data)


for model in models:
    table_name = model.__table__.name
    table_columns = inspector.get_columns(table_name, schema='public')  # Отримання структури таблиці
    collection = mongo_db[table_name]

    # Створення схеми для MongoDB на основі структури таблиці PostgreSQL
    schema = {}
    for column in table_columns:
        schema[column['name']] = str(column['type'])  # Конвертація типів даних PostgreSQL для MongoDB

    # Збереження схеми у колекції MongoDB (якщо вона ще не існує)
    if collection.count_documents({}) == 0:
        collection.insert_one(schema)

    print(f"{table_name} created")
print()

for model, data in zip(models, datas):
    table_name = model.__table__.name
    collection = mongo_db[table_name]

    for item in data:
        item_dict = {column: getattr(item, column) for column in model.__table__.columns.keys()}
        collection.insert_one(item_dict)

    print(f"{table_name} finished")
