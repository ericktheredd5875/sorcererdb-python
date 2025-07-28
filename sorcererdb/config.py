# sorcererdb/config.py

from dataclasses import dataclass

@dataclass
class DBConfig:
    name: str = "MainSorcererDB"
    engine: str = "mysql"
    host: str = "localhost"
    port: int = 3306
    user: str = "sorcerer"
    password: str = "sorcererpw"
    database: str = "sorcererdb"
    charset: str = "utf8mb4"
    timeout: int = 30

