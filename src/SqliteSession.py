import logging, sqlite3, threading

class SqliteSession():
    def __init__(self, db_path, pragma={}, check_same_thread=True):
        self.__logger = logging.getLogger("SqliteSession('{}')".format(db_path))

        self.__lock = threading.Lock()

        self.__cursor = None

        self.__connection = sqlite3.connect(db_path, check_same_thread=check_same_thread)
        self.__connection.row_factory = sqlite3.Row

        self.__cursor = self.__connection.cursor()

        if len(pragma) > 0:
            for (key, value) in pragma.items():
                self.query("PRAGMA {} = {}".format(key, value))

            self.commit()
    
    def __where_builder(self, key):
        where_rep = ""

        for key in key.keys():
            where_rep += "AND {} = ? ".format(key)

        # TODO: Find a better way to remove the first 'AND '
        where_rep = where_rep.replace("AND ", "", 1)

        return where_rep

    def __set_builder(self, update_pairs):
        set_rep = ""

        for update_pairs in update_pairs.keys():
            set_rep += ",{} = ?".format(update_pairs)

        # TODO: Find a better way to remove the first comma
        set_rep = set_rep.replace(",", "", 1)

        return set_rep

    def insert(self, table, values):
        columns = ",".join(values.keys())

        values_rep_string = ",".join('?'*len(values))

        query = 'INSERT INTO {}({}) VALUES ({})'.format(table, columns, values_rep_string) 

        macro_list = list(values.values())

        return self.query(query, macro_list)

    def insert_or_replace(self, table, values):
        columns = ",".join(values.keys())

        values_rep_string = ",".join('?'*len(values))

        query = 'INSERT OR REPLACE INTO {}({}) VALUES ({})'.format(table, columns, values_rep_string) 

        macro_list = list(values.values())

        return self.query(query, macro_list)

    def select(self, table, key, columns="*"):
        macro_list = list(key.values())

        where_rep = self.__where_builder(key)

        query = "SELECT {} FROM {} WHERE {}".format(columns, table, where_rep)

        return self.query(query, macro_list)

    def delete(self, table, key):
        macro_list = list(key.values())

        where_rep = self.__where_builder(key)

        query = "DELETE FROM {} WHERE {}".format(table, where_rep)

        return self.query(query, macro_list)

    def update(self, table, key, update_pairs):
        set_rep = self.__set_builder(update_pairs)
        where_rep = self.__where_builder(key)

        query = "UPDATE {} SET {} WHERE {}".format(table, set_rep, where_rep)

        macro_list = []
        macro_list.extend(list(update_pairs.values()))
        macro_list.extend(list(key.values()))

        return self.query(query, macro_list)

    def query(self, query, macro=[]):
        return self.__cursor.execute(query, macro)

    def commit(self):
        self.__connection.commit()

    def close(self):
        self.__connection.close()

    def lock(self):
        self.__lock.acquire()

    def unlock(self):
        self.__lock.release()