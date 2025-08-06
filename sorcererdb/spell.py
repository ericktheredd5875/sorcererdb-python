# sorcererdb/spell.py
# from re import S
from loguru import logger

import mysql.connector
from mysql.connector import Error

class Spell:

    def __init__(self, conn):
        self.conn     = conn
        self.cursor   = None
        self.query    = None
        self.bindings = None

    def open_cursor(self, query):
        if query.strip().lower().startswith("select"):
            return self.conn.cursor(dictionary=True, buffered=True)
        else:
            return self.conn.cursor(buffered=True)

    def proc(self, name, params = None):
        
        try:
            self.cursor = self.open_cursor("select")
            return self.cursor.callproc(name, params)
        except mysql.connector.Error as err:
            logger.error(f"[Spell] Error executing procedure: {name} | {err}")
            raise ValueError(f"Something went wrong: {err}")

    def execute(self, query, bindings = None):
        
        self.query = query
        self.bindings = bindings
        logger.debug(f"[Spell] Executing query: {self.query} | bindings: {self.bindings}")
        
        try:
            self.cursor = self.open_cursor(query)
            self.cursor.execute(query, bindings)
        except mysql.connector.Error as err:
            logger.error(f"[Spell] Error executing query: {self.query} | {err}")
            raise ValueError(f"Something went wrong: {err}")

        return self.cursor

    def fetch(self, fetch_type = "all", size = 25):
        match fetch_type:
            case "count":
                return self.rowcount()
            case "all":
                return self.fetchall()
            case "one" | "single":
                return self.fetchone()
            case "many":
                return self.fetchmany(size)
            case "insert_id":
                return self.insert_id()
            case _:
                logger.error(f"[Spell] Invalid fetch type: {fetch_type}")
                raise ValueError(f"Invalid fetch type: {fetch_type}")

    def rowcount(self):
        logger.debug(f"[Spell] Rowcount: {self.cursor.rowcount}")
        return self.cursor.rowcount

    def fetchall(self):
        return self.cursor.fetchall()

    def fetchone(self):
        return self.cursor.fetchone()

    def fetchmany(self, size = 25):
        return self.cursor.fetchmany(size)
    
    def insert_id(self):
        return self.cursor.lastrowid
    
    def close(self):
        self.cursor.close()
        self.cursor = None
        logger.debug(f"[Spell] Cursor closed")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def __del__(self):
        if self.cursor:
            self.close()