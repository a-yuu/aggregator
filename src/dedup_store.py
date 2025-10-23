import sqlite3
import os
import asyncio 
from typing import Tuple, Set, Optional, List, Dict, Any
import threading
from datetime import datetime, timezone
from functools import partial 

class DeduplicationStore: 
    """Deduplication store dengan SQLite untuk persistensi asinkronus dan koneksi tunggal."""
    
    def __init__(self, db_path: str = "dedup_store.db"):
        self.db_path = db_path
        self.lock = threading.Lock() 
        # 🔥 PERBAIKAN: Koneksi tunggal, mengizinkan akses dari thread lain
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self._init_db()
    
    def _init_db(self):
        """Initialize database schema (Synchronous setup)"""
        # Menggunakan koneksi instance
        with self.conn as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS processed_events (
                    topic TEXT NOT NULL,
                    event_id TEXT NOT NULL,
                    timestamp TEXT NOT NULL,
                    processed_at TEXT NOT NULL,
                    PRIMARY KEY (topic, event_id)
                )
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_topic 
                ON processed_events(topic)
            """)
            conn.commit()
    
    # 🔥 PERBAIKAN: Metode blocking sekarang menggunakan koneksi self.conn
    def _sync_check(self, topic, event_id):
        with self.lock:
            # Menggunakan koneksi yang sudah dibuka
            cursor = self.conn.execute(
                "SELECT 1 FROM processed_events WHERE topic = ? AND event_id = ?",
                (topic, event_id)
            )
            return cursor.fetchone() is not None

    async def is_processed(self, topic: str, event_id: str) -> bool:
        """Check if event already processed (Async wrapper)"""
        return await asyncio.to_thread(self._sync_check, topic, event_id)
    
    # 🔥 PERBAIKAN: Metode blocking sekarang menggunakan koneksi self.conn
    def _sync_mark(self, topic: str, event_id: str, timestamp: str) -> bool:
        with self.lock:
            try:
                # Menggunakan koneksi yang sudah dibuka
                self.conn.execute(
                    """INSERT INTO processed_events 
                       (topic, event_id, timestamp, processed_at) 
                       VALUES (?, ?, ?, ?)""",
                    (topic, event_id, timestamp, datetime.now(timezone.utc).isoformat())
                )
                self.conn.commit()
                return True
            except sqlite3.IntegrityError:
                return False
    
    async def mark_processed(self, topic: str, event_id: str, timestamp: str) -> bool:
        """Mark event as processed (Async wrapper)"""
        return await asyncio.to_thread(self._sync_mark, topic, event_id, timestamp)

    # 🔥 PERBAIKAN: Metode blocking sekarang menggunakan koneksi self.conn
    def _sync_fetch_events(self, topic: Optional[str]):
        with self.lock:
            # Menggunakan koneksi yang sudah dibuka
            if topic:
                cursor = self.conn.execute(
                    """SELECT topic, event_id, timestamp, processed_at 
                       FROM processed_events WHERE topic = ?
                       ORDER BY processed_at DESC""",
                    (topic,)
                )
            else:
                cursor = self.conn.execute(
                    """SELECT topic, event_id, timestamp, processed_at 
                       FROM processed_events 
                       ORDER BY processed_at DESC"""
                )
            return cursor.fetchall()
            
    async def get_processed_events(self, topic: Optional[str] = None) -> List[Tuple]:
        """Get list of processed events (Async wrapper)"""
        return await asyncio.to_thread(self._sync_fetch_events, topic)

    # 🔥 PERBAIKAN: Metode blocking sekarang menggunakan koneksi self.conn
    def _sync_get_stats(self):
        with self.lock:
            # Menggunakan koneksi yang sudah dibuka
            cursor = self.conn.execute("SELECT COUNT(*) FROM processed_events")
            total = cursor.fetchone()[0]
            
            cursor = self.conn.execute(
                "SELECT topic, COUNT(*) FROM processed_events GROUP BY topic"
            )
            topics = {row[0]: row[1] for row in cursor.fetchall()}
            
            return {"total_unique": total, "topics": topics}

    async def get_stats(self) -> Dict[str, Any]:
        """Get statistics from dedup store (Async wrapper)"""
        return await asyncio.to_thread(self._sync_get_stats)
    
    # 🔥 PERBAIKAN: Metode blocking sekarang menggunakan koneksi self.conn
    def _sync_clear(self):
        with self.lock:
            # Menggunakan koneksi yang sudah dibuka
            self.conn.execute("DELETE FROM processed_events")
            self.conn.commit()

    async def clear(self):
        """Clear all data (Async wrapper)"""
        await asyncio.to_thread(self._sync_clear)
    
    async def close(self):
        """Close database connections (Async wrapper)"""
        def blocking_close():
             with self.lock:
                 self.conn.close()
        await asyncio.to_thread(blocking_close)
