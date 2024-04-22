from sqlalchemy import create_engine, Column, Integer, Float, String, Boolean, Date, Text, ForeignKey, VARCHAR
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv
import os

#Edit the .env file 
dotenv_path = os.path.join('..', 'config', 'postgres_sqlalchemy.env')
load_dotenv(dotenv_path=dotenv_path)

# Enviroment variables with os.getenv
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASS = os.getenv('DB_PASS')
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')

def make_db_engine():
    engine = create_engine(f'postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}')
    return engine

def create_session(engine):
    Session = sessionmaker(bind=engine)
    session = Session()
    return session

def closing_session(session):
    session.close()

def disposing_engine(engine):
    engine.dispose()
