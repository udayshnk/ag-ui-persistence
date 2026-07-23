"""Tests for run_migrations() against SQLite, covering both starting states this
library is ever deployed against: brand-new and a pre-existing host-application-owned
shape that predates this library's own Alembic ownership.
"""
import json

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from ag_ui_persistence import run_migrations


async def _columns(engine, table_name):
    async with engine.connect() as conn:
        result = await conn.execute(text(f"PRAGMA table_info({table_name})"))
        return {row[1] for row in result}


async def _table_exists(engine, table_name):
    async with engine.connect() as conn:
        result = await conn.execute(
            text("SELECT name FROM sqlite_master WHERE type='table' AND name=:name"),
            {"name": table_name},
        )
        return result.fetchone() is not None


@pytest.mark.asyncio
async def test_run_migrations_creates_tables_on_fresh_database(tmp_path):
    db_path = tmp_path / "fresh.db"
    run_migrations(f"sqlite+aiosqlite:///{db_path}")

    engine = create_async_engine(f"sqlite+aiosqlite:///{db_path}")
    try:
        assert await _table_exists(engine, "agui_threads")
        assert await _table_exists(engine, "agui_runs")
        assert await _table_exists(engine, "agui_events")
        assert await _table_exists(engine, "ag_ui_persistence_alembic_version")

        run_cols = await _columns(engine, "agui_runs")
        assert {"run_input", "agent_id", "title", "summary"} <= run_cols

        event_cols = await _columns(engine, "agui_events")
        assert "thread_id" in event_cols
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_run_migrations_is_idempotent(tmp_path):
    db_path = tmp_path / "twice.db"
    run_migrations(f"sqlite+aiosqlite:///{db_path}")
    run_migrations(f"sqlite+aiosqlite:///{db_path}")  # must not raise or duplicate

    engine = create_async_engine(f"sqlite+aiosqlite:///{db_path}")
    try:
        async with engine.connect() as conn:
            result = await conn.execute(text("SELECT COUNT(*) FROM ag_ui_persistence_alembic_version"))
            assert result.scalar() == 1
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_run_migrations_patches_pre_existing_shape_forward(tmp_path):
    """Seed a shape from before this library owned migrations (no run_input/agent_id on
    agui_runs, no thread_id on agui_events) — the shape a host application's own
    migrations used to patch forward before this library took over. run_migrations()
    must bring it to head and backfill with the same values those migrations did.
    """
    db_path = tmp_path / "legacy.db"
    engine = create_async_engine(f"sqlite+aiosqlite:///{db_path}")
    try:
        async with engine.begin() as conn:
            await conn.execute(text(
                "CREATE TABLE agui_threads (thread_id TEXT PRIMARY KEY, namespace TEXT, "
                "latest_run_id TEXT, created_at BIGINT NOT NULL, updated_at BIGINT NOT NULL)"
            ))
            await conn.execute(text(
                "CREATE TABLE agui_runs (run_id TEXT PRIMARY KEY, thread_id TEXT NOT NULL, "
                "parent_run_id TEXT, previous_run_id TEXT, seq INTEGER NOT NULL, "
                "status TEXT NOT NULL, title TEXT, summary TEXT, "
                "created_at BIGINT NOT NULL, updated_at BIGINT NOT NULL)"
            ))
            await conn.execute(text(
                "CREATE TABLE agui_events (run_id TEXT NOT NULL, seq INTEGER NOT NULL, "
                "event_type TEXT NOT NULL, data TEXT NOT NULL, started_at BIGINT NOT NULL, "
                "ended_at BIGINT NOT NULL, PRIMARY KEY (run_id, seq))"
            ))
            await conn.execute(
                text("INSERT INTO agui_threads VALUES (:tid, NULL, :rid, 1, 1)"),
                {"tid": "thread-1", "rid": "run-1"},
            )
            await conn.execute(
                text(
                    "INSERT INTO agui_runs (run_id, thread_id, parent_run_id, previous_run_id, "
                    "seq, status, title, summary, created_at, updated_at) VALUES "
                    "(:rid, :tid, NULL, NULL, 0, 'completed', :title, NULL, 1, 1)"
                ),
                {"rid": "run-1", "tid": "thread-1", "title": "Hello"},
            )
            await conn.execute(
                text(
                    "INSERT INTO agui_runs (run_id, thread_id, parent_run_id, previous_run_id, "
                    "seq, status, title, summary, created_at, updated_at) VALUES "
                    "(:rid, :tid, :prid, NULL, 1, 'completed', :title, NULL, 2, 2)"
                ),
                {"rid": "sub-1", "tid": "thread-1", "prid": "run-1", "title": "wm_backend_expert"},
            )
            await conn.execute(
                text(
                    "INSERT INTO agui_events (run_id, seq, event_type, data, started_at, ended_at) "
                    "VALUES (:rid, 0, 'RUN_STARTED', '{}', 1, 1)"
                ),
                {"rid": "run-1"},
            )
    finally:
        await engine.dispose()

    run_migrations(f"sqlite+aiosqlite:///{db_path}")

    engine = create_async_engine(f"sqlite+aiosqlite:///{db_path}")
    try:
        run_cols = await _columns(engine, "agui_runs")
        assert {"run_input", "agent_id"} <= run_cols
        event_cols = await _columns(engine, "agui_events")
        assert "thread_id" in event_cols

        async with engine.connect() as conn:
            row = (await conn.execute(
                text("SELECT run_input, agent_id FROM agui_runs WHERE run_id = 'run-1'")
            )).fetchone()
            assert json.loads(row[0]) == {"text": "Hello"}
            assert row[1] is None  # top-level run, no parent

            sub_row = (await conn.execute(
                text("SELECT agent_id FROM agui_runs WHERE run_id = 'sub-1'")
            )).fetchone()
            assert sub_row[0] == "wm_backend_expert"

            event_row = (await conn.execute(
                text("SELECT thread_id FROM agui_events WHERE run_id = 'run-1'")
            )).fetchone()
            assert event_row[0] == "thread-1"
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_agui_persistence_initialize_is_noop_after_run_migrations(tmp_path):
    """store.py's own _DDL must stay a harmless no-op once run_migrations() has already
    brought the schema to head — confirms the two mechanisms don't conflict.
    """
    from ag_ui_persistence import AGUIPersistence, PersistenceConfig

    db_path = tmp_path / "combined.db"
    run_migrations(f"sqlite+aiosqlite:///{db_path}")

    store = AGUIPersistence(PersistenceConfig(db_url=f"sqlite:///{db_path}"))
    await store.initialize()
    await store.put_run("thread-1", "run-1", parent_run_id=None, run_input={"text": "hi"})
    threads = await store.get_threads()
    assert len(threads) == 1
    await store.close()


@pytest.mark.asyncio
async def test_run_migrations_url_with_percent_character(tmp_path):
    """Regression: a stage deployment's DB password contained '!*##', which
    render_as_string() percent-encodes into the URL (e.g. '%21%2A'). Passing that
    URL through Config.set_main_option() routes through configparser's set(), which
    applies '%'-interpolation by default and raises ValueError on a bare '%XX'
    sequence — a real production startup failure this caused. _migration_runner.py
    and env.py now read the URL from an env var instead, never through configparser,
    so this must succeed regardless of how the '%' character got into the URL.
    Reproduced here via a '%' in the file path itself, since SQLite URLs have no
    username/password component to attach a real password to.
    """
    db_path = tmp_path / "db%2Fwith%21percent.db"
    run_migrations(f"sqlite+aiosqlite:///{db_path}")

    engine = create_async_engine(f"sqlite+aiosqlite:///{db_path}")
    try:
        assert await _table_exists(engine, "agui_threads")
    finally:
        await engine.dispose()
