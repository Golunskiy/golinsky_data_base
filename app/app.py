from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from flask_migrate import Migrate

USER = "postgres"
PASSWORD = "postgres"
HOST = "db"
DBNAME = "DB_docker"


app = Flask(__name__)

app.config["SQLALCHEMY_DATABASE_URI"] = f"postgresql://{USER}:{PASSWORD}@{HOST}/{DBNAME}"
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False

db = SQLAlchemy(app)
migrate = Migrate(app, db)


__all__ = ['app', 'db']
