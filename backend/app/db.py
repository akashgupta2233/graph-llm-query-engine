import sqlite3
from contextlib import contextmanager
from pathlib import Path
from threading import Lock

from app.config import DB_PATH, DATA_DIR
from app.services.data_loader import DatasetLoader


_db_lock = Lock()
_is_ready = False


def get_connection(db_path: Path = DB_PATH) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def ensure_database_ready(force_reload: bool = False) -> None:
    global _is_ready

    with _db_lock:
        if _is_ready and not force_reload and DB_PATH.exists():
            return

        DATA_DIR.mkdir(parents=True, exist_ok=True)
        loader = DatasetLoader(DB_PATH)
        loader.load(force_reload=force_reload)
        _is_ready = True


@contextmanager
def db_session() -> sqlite3.Connection:
    ensure_database_ready()
    conn = get_connection()
    try:
        yield conn
    finally:
        conn.close()
