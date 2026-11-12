import pytest

from app import db as db_module
from app.seed import init_db


@pytest.fixture()
def isolated_db(tmp_path, monkeypatch):
    """Point every connect() at a fresh per-test SQLite file and initialise it."""
    db_file = tmp_path / "test_app.db"
    monkeypatch.setattr(db_module, "DB_PATH", db_file)
    init_db()
    return db_file
