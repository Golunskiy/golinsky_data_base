from sqlalchemy import create_engine, func, distinct, desc
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.ext.automap import automap_base
from sqlalchemy import select
from flask import Flask, render_template, redirect, request
from pymongo import MongoClient
from bson.objectid import ObjectId


PSQL = "PSQL"
MONGODB = "MONGODB"
CHOICE_PSQL_OR_MONGODB = PSQL


app = Flask(__name__)

# Підключення до PostgreSQL
POSTGRES_USER = "postgres"
POSTGRES_PASSWORD = "postgres"
POSTGRES_HOST = "localhost"
POSTGRES_DBNAME = "DB2"
postgres_engine = create_engine(f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}/{POSTGRES_DBNAME}")
PostgresSession = sessionmaker(bind=postgres_engine)
postgres_session = PostgresSession()

Base = automap_base()
Base.prepare(postgres_engine, reflect=True)

Result = Base.classes.result
PT = Base.classes.pt
Reg = Base.classes.reg
Info = Base.classes.info
EO = Base.classes.eo
MainData2019 = Base.classes.maindata2019

ModelsPSQL = {
    "result": Result,
    "pt": PT,
    "reg": Reg,
    "info": Info,
    "eo": EO,
    "maindata2019": MainData2019
}

session = Session(postgres_engine)
conn = postgres_engine.connect()


# Підключення до MongoDB
MONGO_USER = "golinsky"
MONGO_PASSWORD = "1234567890"
MONGO_CLUSTER = "cluster0.udq9qzo.mongodb.net"
MONGO_DBNAME = "DB_mongo"
mongo_client = MongoClient(f"mongodb+srv://{MONGO_USER}:{MONGO_PASSWORD}@{MONGO_CLUSTER}/{MONGO_DBNAME}")
mongo_db = mongo_client[MONGO_DBNAME]

# Отримання колекцій
result_collection = mongo_db["result"]
pt_collection = mongo_db["pt"]
reg_collection = mongo_db["reg"]
info_collection = mongo_db["info"]
eo_collection = mongo_db["eo"]
maindata2019_collection = mongo_db["maindata2019"]

ModelsMongoDB = {
    "result": result_collection,
    "pt": pt_collection,
    "reg": reg_collection,
    "info": info_collection,
    "eo": eo_collection,
    "maindata2019": maindata2019_collection
}

spaceProblemSolverDict = {
    "Українська_мова_і_література": "Українська мова і література",
    "Історія_України": "Історія України",
    "Математика": "Математика",
    "Фізика": "Фізика",
    "Хімія": "Хімія",
    "Географія": "Географія",
    "Англійська_мова": "Англійська мова",
    "Французька_мова": "Французька мова",
    "Німецька_мова": "Німецька мова",
    "Іспанська_мова": "Іспанська мова"}
subjectDict = {"Українська мова і література": MainData2019.result_ukr_id,
                "Історія України": MainData2019.result_hist_id,
                "Математика": MainData2019.result_math_id,
                "Фізика": MainData2019.result_phys_id,
                "Хімія": MainData2019.result_chem_id,
                "Географія": MainData2019.result_geo_id,
                "Англійська мова": MainData2019.result_eng_id,
                "Французька мова": MainData2019.result_fra_id,
                "Німецька мова": MainData2019.result_deu_id,
                "Іспанська мова": MainData2019.result_spa_id}


# Methods for all tables #

def getColumnNamesPSQL(Model):
    column_names = [column.key for column in Model.__table__.columns]
    return column_names


def getColumnNamesMongoDB(collection):
    column_names = collection.find_one().keys()
    return column_names


def fetchRowsFromTablePSQL(Model):
    if hasattr(Model, 'outid'):
        query = select(Model).order_by(desc(Model.outid)).limit(10)
    else:
        query = select(Model).order_by(desc(Model.id)).limit(10)
    return conn.execute(query)


def fetchRowsFromTableMongoDB(collection):
    if 'outid' in collection.find_one():
        query = collection.find().sort('outid', -1).limit(10)
    else:
        query = collection.find().sort('id', -1).limit(10)
    return query


def fetchTableByIdPSQL(Model, id):
    if hasattr(Model, 'outid'):
        query = session.query(Model).filter(Model.outid == id)
    else:
        query = session.query(Model).filter(Model.id == id)
    return query.first()


def fetchTableByIdMongoDB(collection, id):
    if 'outid' in collection.find_one():
        query = collection.find_one({'outid': id})
    else:
        query = collection.find_one({'id': id})

    return query


def fetchTablePSQL(Model, query_values):
    query_filters = [getattr(Model, key) == value for query_dict in query_values for key, value in query_dict.items()]
    query = session.query(Model).filter(*query_filters)
    return query.first()


def fetchTableMongoDB(collection, query_values):
    query_filters = {key: value for query_dict in query_values for key, value in query_dict.items()}
    query = collection.find_one(query_filters)

    return query


def createTableRowPSQL(Model, data_dict):
    new_entry = Model(**data_dict)
    session.add(new_entry)
    session.commit()


def createTableRowMongoDB(collection, data_dict):
    collection.insert_one(data_dict)


def updateTableRowPSQL(Model, data_dict):
    if "id" in data_dict:
        res = fetchTableByIdPSQL(Model, data_dict['id'])
    else:
        res = fetchTableByIdPSQL(Model, data_dict['outid'])
    if res:
        for key, value in data_dict.items():
            if key != 'id' and key != 'outid':
                # Перевірка на рядок "None" і заміна на None
                if value == 'None':
                    value = None
                setattr(res, key, value)
        session.commit()


def updateTableRowMongoDB(collection, data_dict):
    query = {}

    if "id" in data_dict:
        query['id'] = data_dict['id']
    elif 'outid' in data_dict:
        query['outid'] = data_dict['outid']

    existing_document = collection.find_one(query)
    if existing_document:
        for key, value in data_dict.items():
            if key not in ['id', 'outid', '_id']:
                if value == 'None':
                    value = None
                existing_document[key] = value

        collection.replace_one(query, existing_document)


def deleteTableRowPSQL(Model, id):
    res = fetchTableByIdPSQL(Model, id)
    if res:
        session.delete(res)
        session.commit()


def deleteTableRowMongoDB(collection, id):
    query = {'_id': id}
    existing_document = collection.find_one(query)
    if existing_document:
        collection.delete_one(query)


# Methods for filtering #

def fetchRegnamesPSQL():
    query = select(distinct(PT.regname))
    return tuple(regname[0] for regname in conn.execute(query))


def fetchRegnamesMongoDB():
    distinct_regnames = pt_collection.distinct("regname")
    return tuple(distinct_regnames)


def fetchGradePSQL(regname, subject, function):
    ball = session.query(getattr(func, function)(Result.ball100)). \
        join(MainData2019, Result.id == subjectDict[subject]). \
        join(Reg, MainData2019.reg_id == Reg.id). \
        filter(Reg.regname == regname). \
        filter(Result.test == subject). \
        filter(Result.teststatus == "Зараховано").scalar()

    return ball


def fetchGradeMongoDB(regname, subject, function):
    result = maindata2019_collection.aggregate([
        {
            "$match": {
                "reg_id": {"$in": reg_collection.find({"regname": regname}, {"_id": 0}).distinct("id")}
            }
        },
        {
            "$lookup": {
                "from": "result_collection",
                "localField": "result_ukr_id",
                "foreignField": "id",
                "as": "result_data"
            }
        },
        {
            "$unwind": "$result_data"
        },
        {
            "$match": {
                "result_data.teststatus": "Зараховано",
                "result_data.test": subject
            }
        },
        {
            "$group": {
                "_id": None,
                "min_ball100": {f"${function}": "$result_data.ball100"}
            }
        }
    ])

    # Отримання результату
    for res in result:
        min_ball100 = res["min_ball100"]
        return min_ball100


@app.route("/", methods=['POST', 'GET'])
def showTables():
    global CHOICE_PSQL_OR_MONGODB
    if request.method == 'POST':
        if request.form['submit_button'] != "Filters" \
                and request.form['submit_button'] != "PSQL" \
                and request.form['submit_button'] != "MongoDB":
            if CHOICE_PSQL_OR_MONGODB == PSQL:
                Model = ModelsPSQL[request.form["submit_button"].lower()]
                headers = tuple(getColumnNamesPSQL(Model))
                data = tuple(fetchRowsFromTablePSQL(Model))
            else:
                Model = ModelsMongoDB[request.form["submit_button"].lower()]
                headers = tuple(getColumnNamesMongoDB(Model))
                data = tuple(fetchRowsFromTableMongoDB(Model))
                data = tuple(tuple(d.values()) for d in data)

            return render_template("table.html", headers=headers, data=data,
                                   url=f"/{request.form['submit_button'].lower()}")

        elif request.form['submit_button'] == "Filters":
            if CHOICE_PSQL_OR_MONGODB == PSQL:
                regnames = fetchRegnamesPSQL()
            else:
                regnames = fetchRegnamesMongoDB()
            subjects = tuple(list(spaceProblemSolverDict.keys()))
            functions = ('max', 'min', 'avg')

            return render_template("filters.html", regnames=regnames, subjects=subjects,
                                   functions=functions, url="/filters")

        elif request.form['submit_button'] == "PSQL":
            CHOICE_PSQL_OR_MONGODB = PSQL

        elif request.form['submit_button'] == "MongoDB":
            CHOICE_PSQL_OR_MONGODB = MONGODB

    return render_template("main.html")


def commonFunc():
    url = request.path
    if CHOICE_PSQL_OR_MONGODB == PSQL:
        Model = ModelsPSQL[url[1:]]
        headers = tuple(getColumnNamesPSQL(Model))
        data = tuple(fetchRowsFromTablePSQL(Model))
    else:
        Model = ModelsMongoDB[url[1:]]
        headers = tuple(getColumnNamesMongoDB(Model))
        data = tuple(fetchRowsFromTableMongoDB(Model))
        data = tuple(tuple(d.values()) for d in data)

    if request.method == 'POST':
        my_dict = dict(request.form)

        if 'update_delete' in request.form:
            if request.form['update_delete'] == "Update":
                del my_dict['update_delete']
                if CHOICE_PSQL_OR_MONGODB == PSQL:
                    updateTableRowPSQL(Model, my_dict)
                else:
                    updateTableRowMongoDB(Model, my_dict)

            if request.form['update_delete'] == "Delete":
                if "id" in my_dict:
                    if CHOICE_PSQL_OR_MONGODB == PSQL:
                        deleteTableRowPSQL(Model, request.form['id'])
                    else:
                        deleteTableRowMongoDB(Model, request.form['_id'])
                else:
                    if CHOICE_PSQL_OR_MONGODB == PSQL:
                        deleteTableRowPSQL(Model, request.form['outid'])
                    else:
                        deleteTableRowMongoDB(Model, request.form['_id'])

        else:
            del my_dict['add_data']
            if CHOICE_PSQL_OR_MONGODB == PSQL:
                createTableRowPSQL(Model, my_dict)
            else:
                createTableRowMongoDB(Model, my_dict)

        session.commit()
        return redirect(url)

    return render_template("table.html", headers=headers, data=data, url=url)


@app.route("/maindata2019", methods=['POST', 'GET'])
def maindata2019Table():
    return commonFunc()


@app.route("/result", methods=['POST', 'GET'])
def resultTable():
    return commonFunc()


@app.route("/pt", methods=['POST', 'GET'])
def ptTable():
    return commonFunc()


@app.route("/eo", methods=['POST', 'GET'])
def eoTable():
    return commonFunc()


@app.route("/reg", methods=['POST', 'GET'])
def regTable():
    return commonFunc()


@app.route("/info", methods=['POST', 'GET'])
def infoTable():
    return commonFunc()


@app.route("/filters", methods=["POST", "GET"])
def filters():
    if CHOICE_PSQL_OR_MONGODB == PSQL:
        regnames = fetchRegnamesPSQL()
    else:
        regnames = fetchRegnamesMongoDB()
    subjects = tuple(list(spaceProblemSolverDict.keys()))
    functions = ('max', 'min', 'avg')
    grade = ''

    if request.method == 'POST':
        selected_regname = request.form['regnames']
        selected_subject = request.form['subjects']
        selected_function = request.form['funcs']

        if selected_regname != "м.Київ":
            selected_regname += " область"

        if CHOICE_PSQL_OR_MONGODB == PSQL:
            grade = fetchGradePSQL(selected_regname, spaceProblemSolverDict[selected_subject], selected_function)
        else:
            grade = fetchGradePSQL(selected_regname, spaceProblemSolverDict[selected_subject], selected_function)
        if grade == 0:
            grade = 'None'
        session.commit()

    return render_template("filters.html", regnames=regnames, subjects=subjects, functions=functions,
                           grade=grade, url="/filters")


if __name__ == "__main__":
    #app.run(debug=True)
    app.run(host='0.0.0.0', port=5000, debug=True)