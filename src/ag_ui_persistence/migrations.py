"""Public entry point for bringing this package's own tables to head via Alembic.

Call this BEFORE initialize() (and before any read/write call) whenever the target
database might already exist in a shape older than this library's current head — e.g.
one previously managed by a host application's own migrations, or created by an
earlier version of this library. initialize()'s own CREATE TABLE IF NOT EXISTS /
CREATE INDEX IF NOT EXISTS DDL only ever creates a schema fresh; it never adds a
column to a table that already exists, so a pre-existing database left to
initialize() alone silently stays on its old shape forever.

Not usable with sqlite:///:memory: — this always opens its own separate connection
to the given URL (via a subprocess), and a second connection to a SQLite :memory:
database is a distinct, empty database, invisible to whatever connection the
caller's own engine holds. Use a real file path (even a temp file) or Postgres. For
:memory: or other ephemeral/fresh-only use, initialize() alone is sufficient.
"""

import os
import subprocess
import sys
from typing import Optional

from sqlalchemy.engine import make_url


def run_migrations(url: str, username: Optional[str] = None, password: Optional[str] = None) -> None:
    """Run this package's own migrations to head.

    url: the caller's own async DB URL (e.g. postgresql+asyncpg://... or
    sqlite+aiosqlite://...) — remapped here to the sync driver Alembic needs
    (psycopg / sqlite).
    """
    parsed = make_url(url)
    if username:
        parsed = parsed.set(username=username)
    if password:
        parsed = parsed.set(password=password)
    if parsed.drivername == "postgresql+asyncpg":
        parsed = parsed.set(drivername="postgresql+psycopg")
    elif parsed.drivername == "sqlite+aiosqlite":
        parsed = parsed.set(drivername="sqlite")

    env = os.environ.copy()
    env["AG_UI_PERSISTENCE_ALEMBIC_URL"] = parsed.render_as_string(hide_password=False)

    # No cwd override: this package's own alembic/ subpackage must never land on
    # sys.path[0] (e.g. via cwd being this directory), or it shadows the real,
    # installed `alembic` package that _migration_runner needs to import.
    result = subprocess.run(
        [sys.executable, "-m", "ag_ui_persistence._migration_runner"],
        env=env,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"ag_ui_persistence migrations failed:\n{result.stdout}\n{result.stderr}"
        )
