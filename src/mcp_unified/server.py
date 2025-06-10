import json
import sqlite3
from typing import List, Optional, Any, Coroutine

import oracledb
import requests
import sqlite_vec
from loguru import logger
from mcp.server.fastmcp import FastMCP
from dotenv import dotenv_values

from models.base_model import Metadata
from utils.helpers import serialize_f32
from utils.query import generate_sqlite_table, generate_sqlite_insert, generate_sqlite_select, generate_sqlite_update, \
    generate_sqlite_delete, generate_sqlite_select_all, generate_sqlite_vector, generate_sqlite_insert_vector, \
    generate_sqlite_delete_vector, generate_sqlite_select_vector, generate_sqlite_select_by_id

mcp = FastMCP('unified')

# Global variables
_sqlite_client = None
_sqlite_cursor = None
_oracle_client = None
_oracle_cursor = None
_embedding_provider = None
_embedding_model = None


def get_env():
    return dotenv_values(".env")


def get_sqlite_client():
    """Get or create the global Sqlite3 client instance"""
    global _sqlite_client
    global _sqlite_cursor
    environment = get_env()
    if _sqlite_client is None:
        _sqlite_client = sqlite3.connect(environment.get('SQLITE_DATABASE'))
        _sqlite_client.row_factory = sqlite3.Row
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
        _oracle_client = oracledb.connect(
            user=environment.get('ORACLE_USER'),
            password=environment.get('ORACLE_PASSWORD'),
            dsn=environment.get('ORACLE_DSN'),  # e.g., "localhost/XEPDB1"
        )
        _oracle_cursor = _oracle_client.cursor()
    return _oracle_client, _oracle_cursor


def get_embedding_provider():
    """Get or create the global LLM Embedding client instance"""
    global _embedding_provider
    global _embedding_model
    environment = get_env()
    if _embedding_provider is None:
        _embedding_provider = environment.get('OLLAMA_URL')
        _embedding_model = environment.get('EMBEDDING_MODEL')
    return _embedding_provider, _embedding_model


def create_metadata_table():
    conn, cur = get_sqlite_client()
    try:
        query = generate_sqlite_table()
        cur.execute(query)
        conn.commit()
    except Exception as e:
        raise Exception(f"Failed to create metadatas table: {str(e)}") from e


def create_vector_table():
    conn, cur = get_sqlite_client()
    try:
        query = generate_sqlite_vector()
        cur.execute(query)
        conn.commit()
    except Exception as e:
        raise Exception(f"Failed to create vector table: {str(e)}") from e


##### Collection Tools #####

@mcp.tool()
async def metadata_create(
        table_name: str,
        description: str,
        metadatas: List[Metadata]
) -> str:
    """Create metadata table in the SQLite3 database.

        Args:
            table_name: Name of the table to create
            description: A description of the table that provides an overview of the table itself.
            metadatas: List metadata which describes the schema and metadata of the table, including column names, data types, and descriptions of each column.

    """
    conn, cur = get_sqlite_client()
    try:
        query = generate_sqlite_insert()
        json_metadata = json.dumps([metadata.model_dump_json() for metadata in metadatas])
        cur.execute(query, (table_name, description, json_metadata))
        conn.commit()
        return f"Successfully insert data with name {table_name}"
    except Exception as e:
        raise Exception(f"Failed to insert metadata: {str(e)}") from e


@mcp.tool()
async def metadata_get(
        limit: Optional[int] = 10,
        offset: Optional[int] = 0
) -> List[dict]:
    """List all metadata table names in the SQLite3 database with pagination support.

        Args:
            limit: Optional maximum number of metadata tables to return
            offset: Optional number of metadata tables to skip before returning results

        Returns:
            List of metadata tables
    """
    conn, cur = get_sqlite_client()
    try:
        query = generate_sqlite_select()
        cur.execute(query, (limit, offset))
        rows = cur.fetchall()
        print(rows)
        result = [dict(row) for row in rows]
        return result
    except Exception as e:
        raise Exception(f"Failed to select metadata: {str(e)}") from e


@mcp.tool()
async def metadata_update(
        table_name: str,
        description: str,
        metadata: Metadata
) -> str:
    """Create metadata table in the SQLite3 database.

        Args:
            table_name: Name of the table to update in
            description: A new description of the table that provides an overview of the table itself. Provide previous value if no update
            metadata: A new metadata which describes the schema and metadata of the table, including column names, data types, and descriptions of each column. Provide previous value if no update

    """
    conn, cur = get_sqlite_client()
    try:
        query = generate_sqlite_update()
        json_metadata = metadata.model_dump_json()
        cur.execute(query, (table_name, description, json_metadata, table_name))
        conn.commit()
        return f"Successfully update data with name {table_name} | description {description} | metadata {json_metadata}"
    except Exception as e:
        raise Exception(f"Failed to update metadata: {str(e)}") from e


@mcp.tool()
async def metadata_delete(
        table_name: str
) -> str:
    """Delete metadata table names in the SQLite3 database .

        Args:
            table_name: Name of the table metadata to delete
    """
    conn, cur = get_sqlite_client()
    try:
        query = generate_sqlite_delete()
        cur.execute(query, table_name)
        conn.commit()
        return f"Successfully delete data with name {table_name}"
    except Exception as e:
        raise Exception(f"Failed to delete metadata: {str(e)}") from e


def embedding(data: str) -> dict:
    provider, model = get_embedding_provider()
    headers = {
        'Content-Type': 'application/json',
    }
    json_data = {
        'model': model,
        'input': data
    }

    response = requests.request('POST', provider, headers=headers, data=json.dumps(json_data))
    if response.status_code == 200:
        return response.json()
    return {}


@mcp.tool()
async def sync_metadata():
    """Synchronize and re-create all metadata vector in sqlite database including delete previous metadata and add new metadata"""
    conn, cur = get_sqlite_client()
    try:
        query = generate_sqlite_select_all()
        cur.execute(query)
        rows = cur.fetchall()
        results = [dict(row) for row in rows]
        embedding_results = embedding(json.dumps(results))
        datas = []
        if embedding_results:
            for index, result in enumerate(embedding_results.get('embeddings')):
                datas.append((index + 1, serialize_f32(result)))
        delete_query = generate_sqlite_delete_vector()
        cur.execute(delete_query)
        conn.commit()
        insert_query = generate_sqlite_insert_vector()
        cur.executemany(insert_query, datas)
        conn.commit()
        return f"Successfully sync metadata"
    except Exception as e:
        raise Exception(f"Failed to sync metadata: {str(e)}") from e

@mcp.resource('tables://{prompt}')
async def get_relevant_tables(prompt: str):
    """Resource to get relevant table to get schema and metadata from vector db to improve context.

        Args:
            prompt: Prompt from user input as an identifier for metadata table
        Returns:
            A metadata info from table
    """
    conn, cur = get_sqlite_client()
    try:
        query = generate_sqlite_select_vector()
        embedding_results = embedding(prompt)
        datas = []
        if embedding_results:
            embeddings = embedding_results.get('embeddings')
            datas.append(serialize_f32(embeddings[0]))
        cur.execute(query, datas)
        rows = cur.execute(query, datas).fetchall()
        results = [dict(row) for row in rows]
        result = results[0]
        rowid = result.get('rowid')
        select_query = generate_sqlite_select_by_id()
        cur.execute(select_query, (rowid,))
        rows = cur.fetchall()
        metadata_results = [dict(row) for row in rows]
        return metadata_results[0]
    except Exception as e:
        raise Exception(f"Failed to get relevant table: {str(e)}") from e


@mcp.tool()
async def data_get(query: str) -> List[dict]:
    """Fetch all the result from oracle database with provided query

        Args:
            query: SQL query that LLM generated from user input prompt

        Returns:
            List of result data
    """
    conn, cur = get_oracle_client()
    try:
        cur.execute(query)
        columns = [col[0] for col in cur.description]
        results = [dict(zip(columns, row)) for row in cur.fetchall()]
        return results
    except Exception as e:
        raise Exception(f"Failed to get data from oracle: {str(e)}") from e


def main():
    """Entry point for the Unified MCP server."""
    create_metadata_table()
    create_vector_table()
    print("Successfully Created all required tables")
    try:
        get_sqlite_client()
        get_oracle_client()
        get_embedding_provider()
        print("Successfully initialized All clients")
    except Exception as e:
        print(f"Failed to initialize All client: {str(e)}")
        raise

    # Initialize and run the server
    print("Starting MCP server")
    mcp.run(transport='stdio')


if __name__ == "__main__":
    main()
