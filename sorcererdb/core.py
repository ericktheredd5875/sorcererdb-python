import mysql.connector
from .config import DBConfig

class SorcererDB:
    def __init__(self, config: DBConfig, dsn=':memory:', cache_backend=None, log_queries=False):
        self.config = config
        self.dsn = dsn
        self.log_queries = log_queries
        self.cache = cache_backend
        self.connections = {}
        self.active_connection = None
        self.query = ""
        self.bindings = {}

    def connect(self, name='default'):
        if self.config.engine == 'mysql':
            conn = mysql.connector.connect(
                host=self.config.host,
                port=self.config.port,
                user=self.config.user,
                password=self.config.password,
                database=self.config.database
            )
            self.connections[name] = conn
            self.active_connection = name
        elif self.config.engine == 'sqlite':
            # conn = sqlite3.connect(self.dsn)
            # self.connections[name] = conn
            # self.active_connection = name
            pass
        else:
            raise ValueError(f"Invalid engine: {self.config.engine}")

    def disconnect(self, name='default'):
        if self.config.engine == 'mysql':
            self.connections[name].close()
            del self.connections[name]
            if self.active_connection == name:
                self.active_connection = None
        elif self.config.engine == 'sqlite':
            pass
    
    def get_connection(self, name='default'):
        pass

    def get_active_connection(self):
        return self.active_connection

    def set_query(self, sql):
        self.query = sql
        return self

    def set_bindings(self, params):
        self.bindings = params
        return self

    def execute(self):
        cursor = self.connections[self.active_connection].cursor()
        cursor.execute(self.query, self.bindings or {})
        if self.query.strip().lower().startswith("select"):
            return cursor.fetchall()
        else:
            self.connections[self.active_connection].commit()
            return cursor.rowcount
