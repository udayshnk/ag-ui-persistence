"""Standalone entry point for running this package's own Alembic migrations.

Invoked as `python -m ag_ui_persistence._migration_runner` in a plain sync subprocess,
not the caller's asyncio event loop — Alembic's sync engine and psycopg3's async
support don't mix cleanly with an already-running event loop. The target URL is
passed via the AG_UI_PERSISTENCE_ALEMBIC_URL env var rather than argv.
"""

import os
from pathlib import Path

from alembic import command
from alembic.config import Config


def main() -> None:
    cfg = Config()
    cfg.set_main_option("script_location", str(Path(__file__).parent / "alembic"))
    cfg.set_main_option("sqlalchemy.url", os.environ["AG_UI_PERSISTENCE_ALEMBIC_URL"])
    command.upgrade(cfg, "head")


if __name__ == "__main__":
    main()
