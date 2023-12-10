from sqlalchemy import create_engine, func, distinct, desc
from sqlalchemy.orm import Session
from sqlalchemy.ext.automap import automap_base
from sqlalchemy import select
from flask import Flask, render_template, redirect, request


USER = "postgres"
PASSWORD = "postgres"
HOST = "db"
DBNAME = "DB_docker"

app = Flask(__name__)
engine = create_engine(f"postgresql://{USER}:{PASSWORD}@{HOST}/{DBNAME}")

Base = automap_base()
Base.prepare(engine, reflect=True)

Result = Base.classes.result
PT = Base.classes.pt
Reg = Base.classes.reg
Info = Base.classes.info
EO = Base.classes.eo
MainData2019 = Base.classes.maindata2019

Models = {
    "result": Result,
    "pt": PT,
    "reg": Reg,
    "info": Info,
    "eo": EO,
    "maindata2019": MainData2019
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


session = Session(engine)
conn = engine.connect()


# Methods for all tables #

def getColumnNames(Model):
    column_names = [column.key for column in Model.__table__.columns]
    return column_names


def fetchRowsFromTable(Model):
    if hasattr(Model, 'outid'):
        query = select(Model).order_by(desc(Model.outid)).limit(10)
    else:
        query = select(Model).order_by(desc(Model.id)).limit(10)
    return conn.execute(query)


def fetchTableById(Model, id):
    if hasattr(Model, 'outid'):
        query = session.query(Model).filter(Model.outid == id)
    else:
        query = session.query(Model).filter(Model.id == id)
    return query.first()


def fetchTable(Model, query_values):
    query_filters = [getattr(Model, key) == value for query_dict in query_values for key, value in query_dict.items()]
    query = session.query(Model).filter(*query_filters)
    return query.first()


def createTableRow(Model, data_dict):
    new_entry = Model(**data_dict)
    session.add(new_entry)
    session.commit()


def updateTableRow(Model, data_dict):
    if "id" in data_dict:
        res = fetchTableById(Model, data_dict['id'])
    else:
        res = fetchTableById(Model, data_dict['outid'])
    if res:
        for key, value in data_dict.items():
            if key != 'id' and key != 'outid':
                # Перевірка на рядок "None" і заміна на None
                if value == 'None':
                    value = None
                setattr(res, key, value)
        session.commit()


def deleteTableRow(Model, id):
    res = fetchTableById(Model, id)
    if res:
        session.delete(res)
        session.commit()


# Methods for filtering #

def fetchRegnames():
    query = select(distinct(PT.regname))
    return tuple(regname[0] for regname in conn.execute(query))


def fetchGrade(regname, subject, function):
    ball = session.query(getattr(func, function)(Result.ball100)). \
        join(MainData2019, Result.id == subjectDict[subject]). \
        join(Reg, MainData2019.reg_id == Reg.id). \
        filter(Reg.regname == regname). \
        filter(Result.test == subject). \
        filter(Result.teststatus == "Зараховано").scalar()

    return ball


@app.route("/", methods=['POST', 'GET'])
def showTables():
    if request.method == 'POST':
        if request.form['submit_button'] != "Filters":

            Model = Models[request.form["submit_button"].lower()]
            headers = tuple(getColumnNames(Model))
            data = tuple(fetchRowsFromTable(Model))
            return render_template("table.html", headers=headers, data=data,
                                   url=f"/{request.form['submit_button'].lower()}")

        else:
            regnames = fetchRegnames()
            subjects = tuple(list(spaceProblemSolverDict.keys()))
            functions = ('max', 'min', 'avg')

            return render_template("filters.html", regnames=regnames, subjects=subjects,
                                   functions=functions, url="/filters")

    return render_template("main.html")


def commonFunc():
    url = request.path
    Model = Models[url[1:]]
    headers = tuple(getColumnNames(Model))
    data = tuple(fetchRowsFromTable(Model))
    if request.method == 'POST':
        my_dict = dict(request.form)

        if 'update_delete' in request.form:
            if request.form['update_delete'] == "Update":
                del my_dict['update_delete']
                updateTableRow(Model, my_dict)

            if request.form['update_delete'] == "Delete":
                if "id" in my_dict:
                    deleteTableRow(Model, request.form['id'])
                else:
                    deleteTableRow(Model, request.form['outid'])

        else:
            del my_dict['add_data']
            createTableRow(Model, my_dict)

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
    regnames = fetchRegnames()
    subjects = tuple(list(spaceProblemSolverDict.keys()))
    functions = ('max', 'min', 'avg')
    grade = ''

    if request.method == 'POST':
        selected_regname = request.form['regnames']
        selected_subject = request.form['subjects']
        selected_function = request.form['funcs']

        if selected_regname != "м.Київ":
            selected_regname += " область"

        grade = fetchGrade(selected_regname, spaceProblemSolverDict[selected_subject], selected_function)
        if grade == 0:
            grade = 'None'
        session.commit()

    return render_template("filters.html", regnames=regnames, subjects=subjects, functions=functions,
                           grade=grade, url="/filters")


if __name__ == "__main__":
    #app.run(debug=True)
    app.run(host='0.0.0.0', port=5000, debug=True)