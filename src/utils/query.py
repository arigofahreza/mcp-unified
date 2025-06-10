def generate_sqlite_table() -> str:
    return """
    CREATE TABLE IF NOT EXISTS metadatas (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        table_name VARCHAR(100) NOT NULL,
        description TEXT NOT NULL,
        metadata TEXT NOT NULL
    )
    """


def generate_sqlite_insert() -> str:
    return """
    INSERT INTO metadatas (table_name, description, metadata) 
    VALUES (?, ?, ?)
    """


def generate_sqlite_select() -> str:
    return """
    SELECT * FROM metadatas LIMIT ? OFFSET ?
    """

def generate_sqlite_select_by_id() -> str:
    return """
    SELECT * FROM metadatas where id = ?
    """

def generate_sqlite_update() -> str:
    return """
    UPDATE metadatas SET table_name = ?, description = ?, metadata = ? WHERE table_name = ?
    """


def generate_sqlite_delete() -> str:
    return """
    DELETE FROM metadatas WHERE table_name = ?
    """

def generate_sqlite_select_all() -> str:
    return """
    SELECT table_name, description, metadata FROM metadatas
    """

def generate_sqlite_delete_vector() -> str:
    return """
    DELETE FROM vector_metadata
    """

def generate_sqlite_vector() -> str:
    return """
    CREATE VIRTUAL TABLE IF NOT EXISTS vector_metadata USING vec0(embedding float[1024])
    """

def generate_sqlite_insert_vector() -> str:
    return """
    INSERT INTO vector_metadata(rowid, embedding) VALUES (?, ?)
    """

def generate_sqlite_select_vector() -> str:
    return """
    SELECT
        rowid,
        distance
    FROM vector_metadata
    WHERE embedding MATCH ? AND K = 1
    ORDER BY distance
    """