class SorcererDB:
    def __init__(self, engine='sqlite', dsn=':memory:', cache_backend=None, log_queries=False):
        self.engine = engine
        self.dsn = dsn
        self.log_queries = log_queries
        self.cache = cache_backend
        self.connections = {}
        self.active_connection = None
        self.query = ""
        self.bindings = {}

    def connect(self, name='default'):
        import sqlite3
        conn = sqlite3.connect(self.dsn)
        self.connections[name] = conn
        self.active_connection = name

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
