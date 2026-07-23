"""Alembic environment for ag_ui_persistence's own tables.

Runs against a dedicated version table (ag_ui_persistence_alembic_version) so it never
collides with a host application's own alembic_version row in the same database, and is
scoped via include_name to only ever touch this library's three tables. There is no
alembic.ini on disk.

The connection URL is read directly from the AG_UI_PERSISTENCE_ALEMBIC_URL env var
(set by migrations.run_migrations() before the subprocess starts), not from the Config
object — Config.set_main_option() routes through configparser's set(), which applies
'%'-interpolation by default and raises ValueError on a URL-encoded password containing
'%XX' sequences (a password with '!', '*', or '#' in it, for example). Reading the env
var directly bypasses configparser entirely, matching how wm-agent-server's own
_run_migrations()/env.py has always sourced its URL.
"""

import os

from alembic import context
from sqlalchemy import engine_from_config, pool

config = context.config

target_metadata = None

OWNED_TABLES = {"agui_threads", "agui_runs", "agui_events"}
VERSION_TABLE = "ag_ui_persistence_alembic_version"


def include_name(name, type_, parent_names):
    if type_ == "table":
        return name in OWNED_TABLES
    return True


def get_url() -> str:
    return os.environ["AG_UI_PERSISTENCE_ALEMBIC_URL"]


def run_migrations_offline() -> None:
    context.configure(
        url=get_url(),
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        include_name=include_name,
        include_schemas=False,
        version_table=VERSION_TABLE,
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    configuration = config.get_section(config.config_ini_section, {}) or {}
    configuration["sqlalchemy.url"] = get_url()
    connectable = engine_from_config(
        configuration,
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )
    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            include_name=include_name,
            include_schemas=False,
            version_table=VERSION_TABLE,
        )
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
