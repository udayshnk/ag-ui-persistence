"""baseline

Revision ID: 8f2a1c4d6e90
Revises:
Create Date: 2026-07-23 00:00:00.000000

Brings agui_threads/agui_runs/agui_events to this library's current head shape.
Handles exactly the two states this library has ever been deployed against: brand-new
(nothing exists yet) and wherever a host application's own historical migrations had
already patched these tables to (any point from before this library owned its own
schema, through today's head) — no outside adopters to hypothesize about.
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = '8f2a1c4d6e90'
down_revision: Union[str, Sequence[str], None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _has_column(table_name: str, column_name: str) -> bool:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    if not inspector.has_table(table_name):
        return False
    return any(column["name"] == column_name for column in inspector.get_columns(table_name))


def _add_column_if_not_exists(table_name: str, column_name: str, type_sql: str) -> None:
    """ADD COLUMN IF NOT EXISTS is Postgres-only syntax — SQLite has no such clause."""
    if op.get_context().dialect.name == "postgresql":
        op.execute(f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS {column_name} {type_sql}")
    elif not _has_column(table_name, column_name):
        op.add_column(table_name, sa.Column(column_name, sa.Text()))


def upgrade() -> None:
    dialect = op.get_context().dialect.name

    # 1. Fresh-install baseline, full head shape. No-op (IF NOT EXISTS) when these
    #    tables already exist from a host application's own prior migrations, or from
    #    a previous AGUIPersistence.initialize() call.
    op.execute("""
        CREATE TABLE IF NOT EXISTS agui_threads (
            thread_id     TEXT PRIMARY KEY,
            namespace     TEXT,
            latest_run_id TEXT,
            created_at    BIGINT NOT NULL,
            updated_at    BIGINT NOT NULL
        )
    """)
    op.execute("""
        CREATE TABLE IF NOT EXISTS agui_runs (
            run_id          TEXT PRIMARY KEY,
            thread_id       TEXT NOT NULL REFERENCES agui_threads(thread_id) ON DELETE CASCADE,
            parent_run_id   TEXT,
            previous_run_id TEXT,
            seq             INTEGER NOT NULL,
            status          TEXT NOT NULL,
            title           TEXT,
            agent_id        TEXT,
            summary         TEXT,
            run_input       JSONB,
            created_at      BIGINT NOT NULL,
            updated_at      BIGINT NOT NULL
        )
    """)
    op.execute("""
        CREATE TABLE IF NOT EXISTS agui_events (
            run_id      TEXT NOT NULL,
            thread_id   TEXT NOT NULL,
            seq         INTEGER NOT NULL,
            event_type  TEXT NOT NULL,
            data        TEXT NOT NULL,
            started_at  BIGINT NOT NULL,
            ended_at    BIGINT NOT NULL,
            PRIMARY KEY (run_id, seq)
        )
    """)
    op.execute("CREATE INDEX IF NOT EXISTS idx_agui_threads_ns ON agui_threads(namespace, updated_at)")
    op.execute("CREATE INDEX IF NOT EXISTS idx_agui_runs_thread ON agui_runs(thread_id, created_at)")
    op.execute("CREATE INDEX IF NOT EXISTS idx_agui_runs_parent ON agui_runs(parent_run_id)")
    # idx_agui_events_thread is deferred to after section 2 below — a pre-existing
    # agui_events table (the patch-forward case) has no thread_id column yet at this
    # point, and the index can't be created against a column that doesn't exist.

    # 2. Patch forward any pre-existing shape to head. These same columns/backfills were
    #    previously added by a host application's own migrations, before this library
    #    took over owning these tables' schema — ported here once, in the repo that
    #    actually owns them. Idempotent: a no-op both on a table just created in head
    #    shape above and on one already patched by this same migration in a prior run.

    # run_input JSONB on agui_runs, backfilled from title.
    _add_column_if_not_exists("agui_runs", "run_input", "JSONB")
    if dialect == "postgresql":
        op.execute("""
            UPDATE agui_runs SET run_input = jsonb_build_object('text', title)
            WHERE title IS NOT NULL AND run_input IS NULL
        """)
    else:
        op.execute("""
            UPDATE agui_runs SET run_input = json_object('text', title)
            WHERE title IS NOT NULL AND run_input IS NULL
        """)

    # agent_id on agui_runs, backfilled from title for sub-runs.
    _add_column_if_not_exists("agui_runs", "agent_id", "TEXT")
    op.execute("""
        UPDATE agui_runs SET agent_id = title
        WHERE parent_run_id IS NOT NULL AND agent_id IS NULL
    """)

    # thread_id on agui_events, backfilled from agui_runs, NOT NULL, and the FK on
    # agui_events.run_id dropped (Postgres only — SQLite has no DROP CONSTRAINT
    # syntax, and a fresh SQLite table from step 1 is already FK-less).
    _add_column_if_not_exists("agui_events", "thread_id", "TEXT")
    # UPDATE ... FROM (not a correlated SET subquery + a second, redundant EXISTS
    # lookup) — one join does both jobs: rows with no matching agui_runs row are
    # left alone by the join itself, same as the EXISTS did, but without querying
    # agui_runs twice per row. Matters at scale — this is a full-table backfill over
    # agui_events, which is normally the largest table this library owns. Both
    # run_id columns are already indexed (agui_runs.run_id is its PK; agui_events'
    # PK is (run_id, seq)), so this plans as a single hash/merge join either way.
    # Same syntax works on SQLite (3.33+) and Postgres, so no dialect branch needed.
    op.execute("""
        UPDATE agui_events
        SET thread_id = r.thread_id
        FROM agui_runs r
        WHERE r.run_id = agui_events.run_id
          AND agui_events.thread_id IS NULL
    """)
    if dialect == "postgresql":
        op.execute("ALTER TABLE agui_events ALTER COLUMN thread_id SET NOT NULL")
        op.execute("ALTER TABLE agui_events DROP CONSTRAINT IF EXISTS agui_events_run_id_fkey")
    op.execute("CREATE INDEX IF NOT EXISTS idx_agui_events_thread ON agui_events(thread_id)")


def downgrade() -> None:
    pass  # one-way baseline — no prior revision to restore to
