from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from flask_migrate import Migrate

USER = "postgres"
PASSWORD = "postgres"
HOST = "localhost"
DBNAME_OLD = "DB"
DBNAME_NEW = "DB2"


app = Flask(__name__)

app.config["SQLALCHEMY_DATABASE_URI"] = f"postgresql://{USER}:{PASSWORD}@{HOST}/{DBNAME_OLD}"
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
app.config["SQLALCHEMY_BINDS"] = {
    "db_old": f"postgresql://{USER}:{PASSWORD}@{HOST}/{DBNAME_OLD}",
    "db_new": f"postgresql://{USER}:{PASSWORD}@{HOST}/{DBNAME_NEW}"
}

db = SQLAlchemy(app)
migrate = Migrate(app, db)


__all__ = ['app', 'db']
