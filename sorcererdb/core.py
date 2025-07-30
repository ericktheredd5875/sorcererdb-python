import mysql.connector
from mysql.connector import Error

from .config import DBConfig
from .spell import Spell


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

        self.sql_query      = ""
        self.bindings       = {}
        self.stored_queries = {}
        self.row_count      = 0

        self.set_dsn(config)

    def __del__(self):
        # self.close_cursor()
        temp_connections = self.connections.copy()
        for conn in temp_connections:
            # self.connections[conn.name].close()
            self.disconnect(conn)

        self.connections = {}
        temp_connections = {}

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
            try:
                conn = mysql.connector.connect(
                    host=conn_config.host,
                    port=conn_config.port,
                    user=conn_config.user,
                    password=conn_config.password,
                    database=conn_config.database,
                    charset = conn_config.charset
                )

                if conn_config.autocommit:
                    conn.autocommit = True

                self.connections[conn_config.name] = conn
                self.active_connection = conn_config.name
            except mysql.connector.Error as err:
                raise ConnectionError(f"Failed to connect to {name}: {err}")

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
            self.query(self.stored_queries[key])
            return self
        else:
            raise ValueError(f"Stored query {key} does not exist")

    def query(self, sql):
        self.reset_bindings()
        self.sql_query = sql
        return self

    def reset_query(self):
        self.sql_query = ""
        return self

    def get_query(self):
        return self.sql_query

    # Bindings Methods
    def binding(self, param, value):
        if type(param) == dict or type(param) == list or type(param) == tuple:
            raise ValueError("Bindings must be a single parameter. Use set_bindings.")

        param = str(param).strip()
        value = str(value).strip()

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
            self.binding(param, value)
        
        return self

    def build_bindings(self, data):
        fields = {}
        values = {}

        if type(data) == dict:
            for key, value in data.items():
                val = value
                condition = "="

                if type(value) == list:
                    val = value[0]
                    condition = value[1]

                binder, query, value = self.format_binding(key, val, condition)

                fields[binder] = query
                values[binder] = value
        elif type(data) == list:
            for item in data:
                # [field, value, condition]
                key       = item[0]
                val       = item[1]
                condition = "="
                if len(item) > 2:
                    condition = item[2]                    

                binder, query, value = self.format_binding(key, val, condition)
                fields[binder] = query
                values[binder] = value

        return fields, values

    def format_binding(self, field, value, condition = "="):
        if field == "":
            return ""

        condition = condition.strip().upper()
        if condition == "LIKE" or condition == "NOT LIKE":
            value = "%" + str(value) + "%"
        elif condition == "IN" or condition == "NOT IN":
            value = tuple(value)
        elif condition == "BETWEEN":
            value = tuple(value)
        elif condition == "NOT BETWEEN":
            value = tuple(value)
        elif condition == "IS NULL" or condition == "IS NOT NULL":
            value = None
        elif condition == "IS" or condition == "IS NOT":
            value = value
        else:
            value = value

        field = field.strip().lower()
        binder = self.format_binder(field)
        query = f"{field} {condition} {binder}"

        return field, query, value
    
    def format_for_in(self, tag, data, delimiter = "|" ):
        if tag == "":
            return ""
        
        if type(data) == str and delimiter:
            data = data.split(delimiter)

        bindings = {}
        for i, value in enumerate(data):
            bindings[f"{tag}_{i}"] = value

        return bindings

    def format_in(self, tag, data, delimiter = "|" ):
        bindings = self.format_for_in(tag, data, delimiter)
        if bindings:
            sql_string = f"{tag} IN (" 
            for key, value in bindings.items():
                binder = self.format_binder(key)
                sql_string += f"{binder}, "
                
            
            sql_string = sql_string[:-2]
            sql_string += ")"
            return sql_string, bindings

        return None, None

    def format_binder(self, key):
        if self.config.engine == "mysql":
            return "%(" + key + ")s"
        elif self.config.engine == "sqlite":
            return "@" + key
        elif self.config.engine == "postgresql":
            return "$" + key
        else:
            raise ValueError(f"Invalid engine: {self.config.engine}")

    # Query Execution Methods
    # def set_cursor(self):
    #     if self.query.strip().lower().startswith("select"):
    #         self.cursor = self.connections[self.active_connection].cursor(dictionary=True, buffered=True)
    #     else:
    #         self.cursor = self.connections[self.active_connection].cursor(buffered=True)
    #     return self
    
    # def close_cursor(self):
    #     if self.cursor:
    #         # self.cursor.fetchall()
    #         self.cursor.close()
    #         self.cursor = None
    #     return self

    # Execute a Stored Procedure
    def proc(self, name, params = ()):
        
        try:
            spell = Spell(self.connections[self.active_connection])
            result = spell.proc(name, params)
        except mysql.connector.Error as err:
            print("Something went wrong: {}".format(err))
            self.sql_error = err
            return False

        return result


    def simple(self, query, fetch_type = "all", size = None):
        try:
            self.query(query)
            spell = Spell(self.connections[self.active_connection])
            spell.execute(self.sql_query)
            return spell.fetch(fetch_type, size)
        except mysql.connector.Error as err:
            print("Something went wrong: {}".format(err))
            self.sql_error = err
            return False

    def execute(self):


        spell = Spell(self.connections[self.active_connection])
        spell.execute(self.sql_query, self.bindings or {})
        return spell

        # self.close_cursor().set_cursor();

        # try:
        #     self.cursor.execute(self.query, self.bindings or {})
        # except mysql.connector.Error as err:
        #     print("Something went wrong: {}".format(err))
        #     self.sql_error = err
        #     return False

        # return True

    def result_set(self, fetch_type = "all", size = None):

        spell = self.execute()
        if spell:
            if fetch_type == "all":
                self.row_count = spell.rowcount()
                return spell.fetchall()
            elif fetch_type == "one":
                self.row_count = spell.rowcount()
                return spell.fetchone()
            elif fetch_type == "many":
                self.row_count = spell.rowcount()
                return spell.fetchmany(size=size)
            elif fetch_type == "count":
                self.row_count = spell.rowcount()
                return self.row_count
            elif fetch_type == "last_insert_id":
                return spell.insert_id()
            else:
                raise ValueError(f"Invalid fetch type: {fetch_type}")
        else:
            return False
    
    def result_data(self, fetch_type = "all", size = None):
        if fetch_type == "all":
            return self.cursor.fetchall()
        elif fetch_type == "one":
            return self.cursor.fetchone()
        elif fetch_type == "many":
            return self.cursor.fetchmany(size=size)

    def result_count(self):
        return self.row_count

    # CRUD Methods
    def insert(self, table, data):
        data_count = int(len(data))
        if data_count > 0:
            fields, values = self.build_bindings(data)

            insert_sql = "INSERT INTO `" + table + "` "
            insert_sql += "SET " + ", ".join(fields.values())
            self.query(insert_sql).set_bindings(values)
            return self.result_set("last_insert_id")
        else:
            raise ValueError(f"Invalid data: {data}")

    def update(self, table, data, conditions):
        fields, values = self.build_bindings(data)

        condition_count = int(len(conditions))
        if condition_count > 0:
            c_fields, c_values = self.build_bindings(conditions)
        else:
            c_fields = {}
            c_values = {}

        update_sql = "UPDATE `" + table + "` "
        update_sql += "SET " + ", ".join(fields.values())

        if condition_count > 0:
            update_sql += " WHERE " + ", ".join(c_fields.values())

        self.query(update_sql).set_bindings(values).set_bindings(c_values)
        return self.result_set("count")

    def delete(self, table, conditions, limit = None):
        condition_count = int(len(conditions))
        if condition_count > 0:
            if limit:
                limit = " LIMIT " + str(limit)
            else:
                limit = ""

            c_fields, c_values = self.build_bindings(conditions)

            delete_sql = "DELETE FROM `" + table + "` "
            delete_sql += " WHERE " + ", ".join(c_fields.values())
            delete_sql += limit

            self.query(delete_sql).set_bindings(c_values)
            return self.result_set("count")
        else:
            raise ValueError(f"Invalid conditions: {conditions}")


    # Transactional Methods
    def begin(self):
        self.connections[self.active_connection].start_transaction()
        return self
    
    def commit(self):
        self.connections[self.active_connection].commit()
        return self
    
    def rollback(self):
        self.connections[self.active_connection].rollback()
        return self