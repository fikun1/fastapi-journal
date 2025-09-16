import json
import os
import uuid
import asyncpg
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from dotenv import load_dotenv
from repositories.interface_repository import DatabaseInterface

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise ValueError("DATABASE_URL environment variable is missing")

class PostgresDB(DatabaseInterface):
    @staticmethod
    def datetime_serialize(obj):
        """Convert datetime objects to ISO format for JSON serialization."""
        if isinstance(obj, datetime):
            return obj.isoformat()
        raise TypeError(f"Type {type(obj)} not serializable")

    # Async context manager
    async def __aenter__(self):
        self.pool = await asyncpg.create_pool(DATABASE_URL)
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self.pool.close()

    # Create
    async def create_entry(self, entry_data: Dict[str, Any]) -> Dict[str, Any]:
        async with self.pool.acquire() as conn:
            query = """
             INSERT INTO entries (id, data, created_at, updated_at)
             VALUES ($1, $2, $3, $4)
            RETURNING *
            """
            entry_id = entry_data.get("id") or str(uuid.uuid4())
            created_at = entry_data.get("created_at") or datetime.now(timezone.utc)
            updated_at = entry_data.get("updated_at") or datetime.now(timezone.utc)

            data_json = json.dumps(entry_data, default=PostgresDB.datetime_serialize)

            row = await conn.fetchrow(query, entry_id, data_json, created_at, updated_at)

            if row:
                data = json.loads(row["data"])
                return {
                    "id": row["id"],
                    "work": data["work"],
                    "struggle": data["struggle"],
                    "intention": data["intention"],
                    "created_at": row["created_at"],
                    "updated_at": row["updated_at"]
                }
            return {}


    # Read all
    async def get_entries(self) -> List[Dict[str, Any]]:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch("SELECT * FROM entries")
            entries = []
            for row in rows:
                data = json.loads(row["data"])
                entries.append({
                    "id": row["id"],
                    "work": data["work"],
                    "struggle": data["struggle"],
                    "intention": data["intention"],
                    "created_at": row["created_at"],
                    "updated_at": row["updated_at"]
                })
            return entries

    # Read one
    async def get_entry(self, entry_id: str) -> Optional[Dict[str, Any]]:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow("SELECT * FROM entries WHERE id = $1", entry_id)
            if row:
                data = json.loads(row["data"])
                return {
                    "id": row["id"],
                    "work": data["work"],
                    "struggle": data["struggle"],
                    "intention": data["intention"],
                    "created_at": row["created_at"],
                    "updated_at": row["updated_at"]
                }
            return None

    # Update
    async def update_entry(self, entry_id: str, updated_data: Dict[str, Any]) -> None:
        updated_at = datetime.now(timezone.utc)
        updated_data["id"] = entry_id
        updated_data["updated_at"] = updated_at

        data_json = json.dumps(updated_data, default=PostgresDB.datetime_serialize)

        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE entries 
                SET data = $2, updated_at = $3
                WHERE id = $1
                """,
                entry_id, data_json, updated_at
            )

    # Delete one
    async def delete_entry(self, entry_id: str) -> None:
        async with self.pool.acquire() as conn:
            await conn.execute("DELETE FROM entries WHERE id = $1", entry_id)

    # Delete all
    async def delete_all_entries(self) -> None:
        async with self.pool.acquire() as conn:
            await conn.execute("DELETE FROM entries")
