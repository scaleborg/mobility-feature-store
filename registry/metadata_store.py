import sqlite3
from typing import Optional


class MetadataStore:
    def __init__(self, db_path: str):
        self.conn = sqlite3.connect(db_path)
        self._init_tables()

    def _init_tables(self):
        cursor = self.conn.cursor()

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS feature_views (
                name TEXT,
                version TEXT,
                description TEXT,
                owner TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (name, version)
            )
            """
        )

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS materializations (
                name TEXT,
                version TEXT,
                run_id TEXT,
                status TEXT,
                storage_path TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

        self.conn.commit()

    def register_feature_view(self, name: str, version: str, description: str, owner: str):
        cursor = self.conn.cursor()

        cursor.execute(
            """
            INSERT OR REPLACE INTO feature_views (name, version, description, owner)
            VALUES (?, ?, ?, ?)
            """,
            (name, version, description, owner),
        )

        self.conn.commit()

    def record_materialization(
        self, name: str, version: str, run_id: str, status: str, storage_path: str
    ):
        cursor = self.conn.cursor()

        cursor.execute(
            """
            INSERT INTO materializations (name, version, run_id, status, storage_path)
            VALUES (?, ?, ?, ?, ?)
            """,
            (name, version, run_id, status, storage_path),
        )

        self.conn.commit()

    def list_feature_views(self):
        cursor = self.conn.cursor()
        cursor.execute("SELECT name, version, owner FROM feature_views")
        return cursor.fetchall()
