import logging, sqlite3

from modules.pytg.Manager import Manager

from modules.pytg.ModulesLoader import ModulesLoader

class Sqlite3Manager(Manager):
    @staticmethod
    def initialize():
        Sqlite3Manager.__instance = Sqlite3Manager()

        return

    @staticmethod
    def load():
        return Sqlite3Manager.__instance

    def __init__(self):
        self.__connections = {}
        self.__cursors = {}

        pass

    ######################
    #       Helpers      #
    ######################
    
    @staticmethod
    def __where_builder(key):
        where_rep = ""

        for key in key.keys():
            where_rep += "AND {} = ? ".format(key)

        # TODO: Find a better way to remove the first 'AND '
        where_rep = where_rep.replace("AND ", "", 1)

        return where_rep

    @staticmethod
    def __set_builder(update_pairs):
        set_rep = ""

        for update_pairs in update_pairs.keys():
            set_rep += ",{} = ?".format(update_pairs)

        # TODO: Find a better way to remove the first comma
        set_rep = set_rep.replace(",", "", 1)

        return set_rep

    ######################
    # SQLite3 interface #
    ######################

    def connect(self, debug=False, db_path=None, session="default"):
        # Load configuration
        config_manager = ModulesLoader.load_manager("config")
        settings = config_manager.load_settings_file("sqlite3")

        # Init connection
        if not db_path:
            db_path = settings["db_path"]

        self.__connections[session] = sqlite3.connect(db_path)
        self.__connections[session].row_factory = sqlite3.Row

        self.__cursors[session] = self.__connections[session].cursor()

        # Load pragmas
        if len(settings["pragma"]) > 0:
            for pragma in settings["pragma"].keys():
                self.query("PRAGMA {} = {}".format(pragma, settings["pragma"][pragma]), session=session)

            self.commit(session=session)

        if debug:
            self.__connections[session].set_trace_callback(print)

    def close(self, session="default"):
        self.__connections[session].close()

    def insert(self, table, values, session="default"):
        columns = ",".join(values.keys())

        values_rep_string = ",".join('?'*len(values))

        query = 'INSERT INTO {}({}) VALUES ({})'.format(table, columns, values_rep_string) 

        macro_list = list(values.values())

        return self.query(query, macro_list, session=session)

    def insert_or_replace(self, table, values, session="default"):
        columns = ",".join(values.keys())

        values_rep_string = ",".join('?'*len(values))

        query = 'INSERT OR REPLACE INTO {}({}) VALUES ({})'.format(table, columns, values_rep_string) 

        macro_list = list(values.values())

        return self.query(query, macro_list, session=session)

    def select(self, table, key, columns="*", session="default"):
        macro_list = list(key.values())

        where_rep = self.__where_builder(key)

        query = "SELECT {} FROM {} WHERE {}".format(columns, table, where_rep)

        return self.query(query, macro_list, session=session)

    def delete(self, table, key, session="default"):
        macro_list = list(key.values())

        where_rep = self.__where_builder(key)

        query = "DELETE FROM {} WHERE {}".format(table, where_rep)

        return self.query(query, macro_list, session=session)

    def update(self, table, key, update_pairs, session="default"):
        set_rep = self.__set_builder(update_pairs)
        where_rep = self.__where_builder(key)

        query = "UPDATE {} SET {} WHERE {}".format(table, set_rep, where_rep)

        macro_list = []
        macro_list.extend(list(update_pairs.values()))
        macro_list.extend(list(key.values()))

        return self.query(query, macro_list, session=session)

    def query(self, query, macro=[], session="default"):
        return self.__cursors[session].execute(query, macro)

    def commit(self, session="default"):
        self.__connections[session].commit()