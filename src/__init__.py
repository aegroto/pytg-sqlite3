from .Sqlite3Manager import Sqlite3Manager

def initialize_manager():
    return Sqlite3Manager()

def depends_on():
    return ["config"]