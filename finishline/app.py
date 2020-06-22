from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from finishline.settings import settings
from sqlalchemy import create_engine, ForeignKey
from sqlalchemy import Column, DateTime, Date, Integer, String, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, backref


def create_app(settings):
    app = Flask(__name__)
    app.config["SQLALCHEMY_DATABASE_URI"] = settings["finishline.sqlalchemy.engine.uri"]
    app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
    app.config["SQLALCHEMY_ECHO"] = settings["finishline.sqlalchemy.engine.echo"]
    db = SQLAlchemy(app)
    return app, db


app, db = create_app(settings)

class Server(db.Model):
    __tablename__ = "server"
    id = Column(Integer, primary_key=True)
    name = Column(String)
    showname = Column(String)


class Jobsonl(db.Model):
    __tablename__ = "jobsonl"
    id = Column(Integer, primary_key=True)
    server = Column(String)
    name = Column(String)
    timestamp = Column(DateTime)
    date = Column(Date)
    status = Column(String)
    raw = Column(String)


class State(db.Model):
    __tablename__ = "jobstate"
    id = Column(Integer, primary_key=True)
    parent_id = Column(Integer, ForeignKey('jobstate.id'))
    expected_start = Column(DateTime)
    expected_end = Column(DateTime)
    actual_start = Column(DateTime)
    actual_end = Column(DateTime)
    server = Column(String)
    jobdate = Column(Date)
    name = Column(String)
    status = Column(String)
    statustxt = Column(String)
    children = relationship("State",
        backref=backref('parent', remote_side=[id])
    )


class Stack(db.Model):
    __tablename__ = "jobstack"
    id = Column(Integer, primary_key=True)
    state_id = Column(Integer, ForeignKey('jobstate.id'))
    server = Column(String)
    name = Column(String)
    state = relationship("State")


class Job(db.Model):
    __tablename__ = "job"
    id = Column(Integer, primary_key=True)
    name = Column(String)
    showname = Column(String)
    indicator = Column(String, unique=True)


class ServerJob(db.Model):
    __tablename__ = "serverjob"
    id = Column(Integer, primary_key=True)
    server_id = Column(Integer, ForeignKey("server.id"))
    job_id = Column(Integer, ForeignKey("job.id"))
    enabled = Column(Boolean)
    timestamp = Column(DateTime)
    text = Column(String)
    server = relationship("Server")
    job = relationship("Job")

