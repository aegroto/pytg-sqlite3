import logging

from modules.sqlite3.Sqlite3Manager import Sqlite3Manager

def initialize():
    logging.info("Initializing sqlite3 module...")

    Sqlite3Manager.initialize()

def connect():
    pass

def load_manager():
    return Sqlite3Manager.load()

def depends_on():
    return ["config"]