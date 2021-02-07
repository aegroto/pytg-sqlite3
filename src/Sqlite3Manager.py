from pytg.Manager import Manager
from pytg.load import manager, get_module_content_folder

from .SqliteSession import SqliteSession

class Sqlite3Manager(Manager):
    def __init__(self):
        self.__sessions = {}

    ######################
    # SQLite3 interface #
    ######################

    def create_session(self, module_name, storage_id, session_id=None, pragma=None, thread_safe=True):
        if not session_id:
            session_id = self.__build_session_id(module_name, storage_id)

        if session_id not in self.__sessions.keys():
            settings = manager("config").load_settings("sqlite3")

            if not pragma:
                pragma = settings["pragma"]

            db_path = "{}/sqlite3/{}.db".format(get_module_content_folder(module_name), storage_id)

            self.__sessions[session_id] = SqliteSession(db_path, pragma, check_same_thread=thread_safe)

        return self.__sessions[session_id]

    def clear_session(self, module_name, storage_id, session_id=None):
        if not session_id:
            session_id = self.__build_session_id(module_name, storage_id)

        self.__sessions[session_id].close()

        del self.__sessions[session_id]

    @staticmethod
    def __build_session_id(module_name, storage_id):
        return "{}_{}".format(module_name, storage_id)