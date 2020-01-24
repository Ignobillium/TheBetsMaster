import pytest
import sqlite3

from database_worker import DataBaseWorker

class TestDataBaseWorker:
    dbw = DataBaseWorker(
        'data/for_tests/test_db.sqlite3',
        'matches',
        'data/for_tests/test_em.sqlite3',
        'ended'
    )

    @pytest.mark.asyncio
    async def test_write_data(self):
        pass

    @pytest.mark.asyncio
    async def test_mark_match_as_ended(self):
        pass
