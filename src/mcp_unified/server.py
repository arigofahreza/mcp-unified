import sqlite3

import sqlite_vec
from mcp.server.fastmcp import FastMCP
from dotenv import dotenv_values

mcp = FastMCP('unified')

# Global variables
_sqlite_client = None
_sqlite_cursor = None
_oracle_client = None
_oracle_cursor = None

def get_env():
    return dotenv_values(".env")

def get_sqlite_client():
    """Get or create the global Sqlite3 client instance"""
    global _sqlite_client
    global _sqlite_cursor
    environment = get_env()
    if _sqlite_client is None:
        _sqlite_client = sqlite3.connect(environment.get('SQLITE_DATABASE'))
        _sqlite_client.execute("PRAGMA journal_mode=WAL")
        _sqlite_cursor = _sqlite_client.cursor()
        _sqlite_client.enable_load_extension(True)
        sqlite_vec.load(_sqlite_client)
        _sqlite_client.enable_load_extension(False)
    return _sqlite_client, _sqlite_cursor

def get_oracle_client():
    """Get or create the global Oracle client instance"""
    global _oracle_client
    global _oracle_cursor
    environment = get_env()
    if _oracle_client is None:
        conn = oracledb.connect(
            user="CDC_KAFKA",
            password="12345678",
            dsn="10.10.10.10/MWNPDB1",  # e.g., "localhost/XEPDB1"
        )
        cursor = conn.cursor()




