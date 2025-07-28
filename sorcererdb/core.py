import mysql.connector
from mysql.connector import Error

from .config import DBConfig

class SorcererDB:
    def __init__(self, config: DBConfig, cache_backend=None, log_queries=False):
        self.config            = config
        self.dsn               = {}
        self.connections       = {}
        self.active_connection = None
        self.cursor            = None

        self.log_queries = log_queries
        self.cache       = cache_backend
        self.sql_error   = None

        self.query          = ""
        self.bindings       = {}
        self.stored_queries = {}
        self.query_count    = 0

        self.set_dsn(config)

    # DSN and Credentials Methods
    def set_dsn(self, config: DBConfig):
        if config.name not in self.dsn:
            if config.name == "":
                config.name = "PDODB-" + str(len(self.dsn) + 1)
            self.dsn[config.name] = config
        else:
            raise ValueError(f"DSN {config.name} already exists")
        
        return self
    
    def get_dsn(self, name):
        if name in self.dsn:
            return self.dsn[name]
        else:
            raise ValueError(f"DSN {name} does not exist")

    def check_dsn(self, name):
        if name in self.dsn:
            return True
        else:
            return False

    # Connection Methods
    def get_connection(self, name):
        if name in self.connections:
            return self.connections[name]
        else:
            raise ValueError(f"Connection {name} does not exist")

    def get_active_connection(self):
        return self.active_connection
    
    def get_connection_name(self):
        return self.dsn[self.active_connection].name
    
    # Change the active db connection based on name
    def set_active_connection(self, name):
        # Check if the connection already exists
        if name in self.connections:
            self.active_connection = name
        # Check if the DSN exists
        elif self.check_dsn(name):
            # Open a new connection if not already open
            self.connect(name)
        else:
            raise ValueError(f"DSN {name} does not exist")
        
        return self

    def check_connection(self, name):
        if name in self.connections:
            return True
        else:
            return False
        

    def connect(self, name):
        conn_config = self.get_dsn(name) 
        if conn_config.engine == 'mysql':
            conn = mysql.connector.connect(
                host=conn_config.host,
                port=conn_config.port,
                user=conn_config.user,
                password=conn_config.password,
                database=conn_config.database,
                charset = conn_config.charset
            )
            self.connections[conn_config.name] = conn
            self.active_connection = conn_config.name
        elif conn_config.engine == 'sqlite':
            # conn = sqlite3.connect(self.dsn)
            # self.connections[name] = conn
            # self.active_connection = name
            pass
        else:
            raise ValueError(f"Invalid engine: {conn_config.engine}")

        return self

    # Disconnect Methods
    def disconnect(self, name):
        conn_config = self.get_dsn(name)
        if conn_config.engine == 'mysql':
            self.connections[conn_config.name].close()
            del self.connections[conn_config.name]
            if self.active_connection == conn_config.name:
                self.active_connection = None
        elif conn_config.engine == 'sqlite':
            pass
    
    

    # Query Methods
    def add_stored_query(self, key, sql):
        self.stored_queries[key] = sql
        return self
    
    def set_stored_query(self, key):
        if key in self.stored_queries:
            self.set_query(self.stored_queries[key])
            return self
        else:
            raise ValueError(f"Stored query {key} does not exist")

    def set_query(self, sql):
        self.query = sql
        return self

    def reset_query(self):
        self.query = ""
        return self

    def get_query(self):
        return self.query

    # Bindings Methods
    def set_binding(self, param, value):
        if type(param) == dict or type(param) == list or type(param) == tuple:
            raise ValueError("Bindings must be a single parameter. Use set_bindings.")

        param = param.strip()
        value = value.strip()

        if "limit" == param or "offset" == param:
            self.bindings[param] = int(value)
        else:
            self.bindings[param] = value


        return self

    def get_bindings(self):
        return self.bindings

    def reset_bindings(self):
        self.bindings = {}
        return self

    def set_bindings(self, params):
        for param, value in params.items():
            self.set_binding(param, value)
        
        return self

    

    def execute(self):

        if self.query.strip().lower().startswith("select"):
            self.cursor = self.connections[self.active_connection].cursor(dictionary=True)
        else:
            self.cursor = self.connections[self.active_connection].cursor()

        try:
            self.cursor.execute(self.query, self.bindings or {})
        except mysql.connector.Error as err:
            print("Something went wrong: {}".format(err))
            self.sql_error = err
            return False

        return True

        # if self.query.strip().lower().startswith("insert") or self.query.strip().lower().startswith("update"):
        #     self.connections[self.active_connection].commit()
        #     return cursor.rowcount
        # else:
        #     return cursor.fetchall()
        # else:
        #     self.connections[self.active_connection].commit()
        #     return cursor.rowcount

    def get_result_set(self, fetch_type = "all", size = None):

        self.cursor.close()
        if self.execute():
            if fetch_type == "all":
                self.query_count = self.cursor.rowcount
                return self.cursor.fetchall()
            elif fetch_type == "one":
                self.query_count = self.cursor.rowcount
                return self.cursor.fetchone()
            elif fetch_type == "many":
                self.query_count = self.cursor.rowcount
                return self.cursor.fetchmany(size=size)
            elif fetch_type == "count":
                self.query_count = self.cursor.rowcount
                return self.query_count
            elif fetch_type == "last_insert_id":
                return self.cursor.lastrowid
            else:
                raise ValueError(f"Invalid fetch type: {fetch_type}")
        else:
            return False




    # def get_last_error(self):