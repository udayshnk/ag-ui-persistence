"""Tests for AGUIPersistence using in-memory SQLite."""
import asyncio
import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine
from ag_ui_persistence import AGUIPersistence, PersistenceConfig, Thread, Run, Event


def mem_store(**kwargs) -> AGUIPersistence:
    return AGUIPersistence(PersistenceConfig(db_url="sqlite:///:memory:", **kwargs))


@pytest_asyncio.fixture
async def store():
    s = mem_store(persist_on_completion=False, merge_delta_events=False)
    await s.initialize()
    try:
        yield s
    finally:
        await s.close()


@pytest.mark.asyncio
async def test_put_and_get_thread(store):
    await store.put_run("thread-1", "run-1", parent_run_id=None, title="Hello", run_input={"text": "test"})
    threads = await store.get_threads()
    assert len(threads) == 1
    assert threads[0].thread_id == "thread-1"
    assert threads[0].namespace is None


@pytest.mark.asyncio
async def test_namespace_stored_on_thread(store):
    await store.put_run("thread-1", "run-1", parent_run_id=None, namespace="project-abc", run_input={"text": "test"})
    threads = await store.get_threads()
    assert threads[0].namespace == "project-abc"


@pytest.mark.asyncio
async def test_namespace_filter(store):
    await store.put_run("thread-1", "run-1", parent_run_id=None, namespace="project-abc", run_input={"text": "test"})
    await store.put_run("thread-2", "run-2", parent_run_id=None, namespace="project-xyz", run_input={"text": "test"})
    await store.put_run("thread-3", "run-3", parent_run_id=None, namespace="project-abc", run_input={"text": "test"})

    abc = await store.get_threads(namespace="project-abc")
    assert {t.thread_id for t in abc} == {"thread-1", "thread-3"}

    xyz = await store.get_threads(namespace="project-xyz")
    assert {t.thread_id for t in xyz} == {"thread-2"}


@pytest.mark.asyncio
async def test_namespace_not_overwritten_on_subsequent_runs(store):
    await store.put_run("thread-1", "run-1", parent_run_id=None, namespace="original", run_input={"text": "test"})
    await store.put_run("thread-1", "run-2", parent_run_id=None, namespace="other", run_input={"text": "test"})
    threads = await store.get_threads()
    assert threads[0].namespace == "original"


@pytest.mark.asyncio
async def test_no_namespace_filter_returns_all(store):
    await store.put_run("thread-1", "run-1", parent_run_id=None, namespace="ns-a", run_input={"text": "test"})
    await store.put_run("thread-2", "run-2", parent_run_id=None, namespace="ns-b", run_input={"text": "test"})
    all_threads = await store.get_threads()
    assert len(all_threads) == 2


@pytest.mark.asyncio
async def test_put_run_creates_thread_once(store):
    await store.put_run("thread-1", "run-1", parent_run_id=None, run_input={"text": "test"})
    await store.put_run("thread-1", "run-2", parent_run_id="run-1", run_input={"text": "test"})
    threads = await store.get_threads()
    assert len(threads) == 1  # thread created only once


@pytest.mark.asyncio
async def test_get_runs_top_level(store):
    await store.put_run("thread-1", "run-1", parent_run_id=None, title="Master", run_input={"text": "test"})
    await store.put_run("thread-1", "run-2", parent_run_id="run-1", title="Sub", run_input={"text": "test"})
    runs = await store.get_runs("thread-1")
    assert len(runs) == 1
    assert runs[0].run_id == "run-1"


@pytest.mark.asyncio
async def test_get_runs_by_parent(store):
    await store.put_run("thread-1", "run-1", parent_run_id=None, run_input={"text": "test"})
    await store.put_run("thread-1", "run-2", parent_run_id="run-1", run_input={"text": "test"})
    await store.put_run("thread-1", "run-3", parent_run_id="run-1", run_input={"text": "test"})
    sub_runs = await store.get_runs("thread-1", parent_run_id="run-1")
    assert len(sub_runs) == 2
    assert {r.run_id for r in sub_runs} == {"run-2", "run-3"}


@pytest.mark.asyncio
async def test_get_runs_respects_namespace(store):
    await store.put_run("thread-1", "run-1", parent_run_id=None, namespace="project-a", run_input={"text": "test"})
    await store.put_run("thread-2", "run-2", parent_run_id=None, namespace="project-b", run_input={"text": "test"})

    runs = await store.get_runs("thread-1", namespace="project-a")
    assert [run.run_id for run in runs] == ["run-1"]

    hidden = await store.get_runs("thread-1", namespace="project-b")
    assert hidden == []


@pytest.mark.asyncio
async def test_update_run_status(store):
    await store.put_run("thread-1", "run-1", parent_run_id=None, run_input={"text": "test"})
    await store.update_run("run-1", status="completed", summary="Done")
    runs = await store.get_runs("thread-1")
    assert runs[0].status == "completed"
    assert runs[0].summary == "Done"


@pytest.mark.asyncio
async def test_update_run_flushes_buffered_events_before_terminal_status(store):
    await store.put_run("thread-1", "run-1", parent_run_id=None, run_input={"text": "test"})
    await store.put_event("run-1", "thread-1", 0, "RUN_STARTED", {"threadId": "thread-1"})
    await store.update_run("run-1", status="completed", summary="Done")

    async with store._engine.connect() as conn:
        event_count = await conn.execute(
            text("SELECT COUNT(*) FROM agui_events WHERE run_id = :rid"),
            {"rid": "run-1"},
        )
        run_status = await conn.execute(
            text("SELECT status FROM agui_runs WHERE run_id = :rid"),
            {"rid": "run-1"},
        )
        assert event_count.scalar() == 1
        assert run_status.scalar() == "completed"


@pytest.mark.asyncio
async def test_get_run_respects_namespace(store):
    await store.put_run("thread-1", "run-1", parent_run_id=None, namespace="project-a", run_input={"text": "test"})

    run = await store.get_run("run-1", namespace="project-a")
    assert run is not None
    assert run.run_id == "run-1"

    hidden = await store.get_run("run-1", namespace="project-b")
    assert hidden is None


@pytest.mark.asyncio
async def test_put_and_get_events(store):
    await store.put_run("thread-1", "run-1", parent_run_id=None, run_input={"text": "test"})
    await store.put_event("run-1", "thread-1", 0, "RUN_STARTED", {"threadId": "thread-1"})
    await store.put_event("run-1", "thread-1", 1, "TEXT_MESSAGE_START", {"messageId": "msg-1"})
    await store.put_event("run-1", "thread-1", 2, "TEXT_MESSAGE_END", {"messageId": "msg-1"})

    events = await store.get_events("run-1")
    assert len(events) == 3
    assert events[0].event_type == "RUN_STARTED"
    assert events[1].seq == 1
    assert events[2].data == {"messageId": "msg-1"}


@pytest.mark.asyncio
async def test_duplicate_event_ignored(store):
    await store.put_run("thread-1", "run-1", parent_run_id=None, run_input={"text": "test"})
    await store.put_event("run-1", "thread-1", 0, "RUN_STARTED", {"threadId": "thread-1"})
    await store.put_event("run-1", "thread-1", 0, "RUN_STARTED", {"threadId": "thread-1"})  # duplicate
    events = await store.get_events("run-1")
    assert len(events) == 1


@pytest.mark.asyncio
async def test_get_events_respects_namespace(store):
    await store.put_run("thread-1", "run-1", parent_run_id=None, namespace="project-a", run_input={"text": "test"})
    await store.put_event("run-1", "thread-1", 0, "RUN_STARTED", {"threadId": "thread-1"})

    events = await store.get_events("run-1", namespace="project-a")
    assert len(events) == 1

    hidden = await store.get_events("run-1", namespace="project-b")
    assert hidden == []


@pytest.mark.asyncio
async def test_namespace_mismatch_does_not_flush_buffered_events(store):
    await store.put_run("thread-1", "run-1", parent_run_id=None, namespace="project-a", run_input={"text": "test"})
    await store.put_event("run-1", "thread-1", 0, "RUN_STARTED", {"threadId": "thread-1"})

    hidden = await store.get_events("run-1", namespace="project-b")
    assert hidden == []

    async with store._engine.connect() as conn:
        result = await conn.execute(
            text("SELECT COUNT(*) FROM agui_events WHERE run_id = :rid"),
            {"rid": "run-1"},
        )
        assert result.scalar() == 0


@pytest.mark.asyncio
async def test_put_event_is_buffered_until_get_events_flushes(store):
    await store.put_run("thread-1", "run-1", parent_run_id=None, run_input={"text": "test"})
    await store.put_event("run-1", "thread-1", 0, "RUN_STARTED", {"threadId": "thread-1"})

    async with store._engine.connect() as conn:
        result = await conn.execute(
            text("SELECT COUNT(*) FROM agui_events WHERE run_id = :rid"),
            {"rid": "run-1"},
        )
        assert result.scalar() == 0

    events = await store.get_events("run-1")
    assert [event.seq for event in events] == [0]

    async with store._engine.connect() as conn:
        result = await conn.execute(
            text("SELECT COUNT(*) FROM agui_events WHERE run_id = :rid"),
            {"rid": "run-1"},
        )
        assert result.scalar() == 1


@pytest.mark.asyncio
async def test_close_flushes_pending_events(tmp_path):
    db_path = tmp_path / "buffered-close.db"
    store = AGUIPersistence(PersistenceConfig(db_url=f"sqlite:///{db_path}", persist_on_completion=False))
    await store.initialize()
    await store.put_run("thread-1", "run-1", parent_run_id=None, run_input={"text": "test"})
    await store.put_event("run-1", "thread-1", 0, "RUN_STARTED", {"threadId": "thread-1"})

    await store.close()

    async with store._engine.connect() as conn:
        result = await conn.execute(
            text("SELECT COUNT(*) FROM agui_events WHERE run_id = :rid"),
            {"rid": "run-1"},
        )
        assert result.scalar() == 1


@pytest.mark.asyncio
async def test_close_disables_store_methods():
    store = mem_store(enable_event_buffering=False, persist_on_completion=False)
    await store.initialize()
    await store.close()

    with pytest.raises(RuntimeError, match="closed"):
        await store.put_run("thread-1", "run-1", parent_run_id=None, run_input={"text": "test"})

    with pytest.raises(RuntimeError, match="closed"):
        await store.get_threads()


@pytest.mark.asyncio
async def test_put_event_writes_immediately_when_buffering_disabled():
    store = mem_store(enable_event_buffering=False, persist_on_completion=False)
    await store.initialize()
    try:
        await store.put_run("thread-1", "run-1", parent_run_id=None, run_input={"text": "test"})
        await store.put_event("run-1", "thread-1", 0, "RUN_STARTED", {"threadId": "thread-1"})

        async with store._engine.connect() as conn:
            result = await conn.execute(
                text("SELECT COUNT(*) FROM agui_events WHERE run_id = :rid"),
                {"rid": "run-1"},
            )
            assert result.scalar() == 1
    finally:
        await store.close()


@pytest.mark.asyncio
async def test_get_events_reads_without_buffer_flush_when_disabled():
    store = mem_store(enable_event_buffering=False, persist_on_completion=False)
    await store.initialize()
    try:
        await store.put_run("thread-1", "run-1", parent_run_id=None, run_input={"text": "test"})
        await store.put_event("run-1", "thread-1", 0, "RUN_STARTED", {"threadId": "thread-1"})

        events = await store.get_events("run-1")
        assert len(events) == 1
        assert events[0].seq == 0
    finally:
        await store.close()


@pytest.mark.asyncio
async def test_buffered_write_failures_surface_and_poison_future_operations():
    """agui_events.run_id has no FK to agui_runs (Part A) — an unregistered run_id
    no longer fails on its own. Use a NULL thread_id instead to violate agui_events'
    NOT NULL constraint and induce a genuine write failure."""
    store = mem_store(persist_on_completion=False)
    await store.initialize()
    try:
        await store.put_event("bad-run", None, 0, "RUN_STARTED", {"threadId": "thread-1"})

        with pytest.raises(Exception):
            await store.get_events("bad-run")

        with pytest.raises(RuntimeError, match="Buffered event write failed"):
            await store.get_threads()
    finally:
        try:
            await store.close()
        except Exception:
            pass


@pytest.mark.asyncio
async def test_close_surfaces_prior_buffered_write_failure():
    store = mem_store(persist_on_completion=False)
    await store.initialize()
    await store.put_event("bad-run", None, 0, "RUN_STARTED", {"threadId": "thread-1"})

    with pytest.raises(Exception):
        await store._flush_all_events()

    with pytest.raises(Exception):
        await store.close()


@pytest.mark.asyncio
async def test_failed_multi_run_batch_still_flushes_other_runs():
    store = mem_store(event_flush_interval=10)
    await store.initialize()
    try:
        await store.put_run("thread-1", "good-run", parent_run_id=None, run_input={"text": "test"})
        await store.put_event("good-run", "thread-1", 0, "RUN_STARTED", {"threadId": "thread-1"})
        await store.put_event("bad-run", None, 0, "RUN_STARTED", {"threadId": "thread-1"})

        with pytest.raises(Exception):
            await store._flush_all_events()

        async with store._engine.connect() as conn:
            result = await conn.execute(
                text("SELECT COUNT(*) FROM agui_events WHERE run_id = :rid"),
                {"rid": "good-run"},
            )
            assert result.scalar() == 1
    finally:
        try:
            await store.close()
        except Exception:
            pass


@pytest.mark.asyncio
async def test_completed_flusher_task_is_restarted_for_new_flushes():
    store = mem_store(event_flush_interval=10, persist_on_completion=False)
    await store.initialize()
    try:
        await store.put_run("thread-1", "run-1", parent_run_id=None, run_input={"text": "test"})
        await store.put_event("run-1", "thread-1", 0, "RUN_STARTED", {"threadId": "thread-1"})

        done_task = asyncio.create_task(asyncio.sleep(0))
        await done_task
        store._flusher_task = done_task

        events = await store.get_events("run-1")
        assert len(events) == 1
        assert events[0].seq == 0
    finally:
        await store.close()


@pytest.mark.asyncio
async def test_get_events_waits_for_pending_tokens_while_same_run_is_inflight():
    store = mem_store(event_batch_size=1, event_flush_interval=10, persist_on_completion=False)
    await store.initialize()
    try:
        await store.put_run("thread-1", "run-1", parent_run_id=None, run_input={"text": "test"})

        started = asyncio.Event()
        release = asyncio.Event()
        original_flush_batch = store._flush_batch

        async def controlled_flush(batch):
            if batch["run-1"][0].seq == 0:
                started.set()
                await release.wait()
            await original_flush_batch(batch)

        store._flush_batch = controlled_flush

        await store.put_event("run-1", "thread-1", 0, "RUN_STARTED", {"threadId": "thread-1"})
        await started.wait()
        await store.put_event("run-1", "thread-1", 1, "TEXT_MESSAGE_END", {"threadId": "thread-1"})

        read_task = asyncio.create_task(store.get_events("run-1"))
        await asyncio.sleep(0)
        assert not read_task.done()

        release.set()
        events = await read_task
        assert [event.seq for event in events] == [0, 1]
    finally:
        await store.close()


@pytest.mark.asyncio
async def test_delete_thread_waits_for_inflight_event_batches(store):
    await store.put_run("thread-1", "run-1", parent_run_id=None, run_input={"text": "test"})

    started = asyncio.Event()
    release = asyncio.Event()
    original_flush_batch = store._flush_batch

    async def controlled_flush(batch):
        started.set()
        await release.wait()
        await original_flush_batch(batch)

    store._flush_batch = controlled_flush

    await store.put_event("run-1", "thread-1", 0, "RUN_STARTED", {"threadId": "thread-1"})
    await started.wait()

    delete_task = asyncio.create_task(store.delete_thread("thread-1"))
    await asyncio.sleep(0)
    assert not delete_task.done()

    release.set()
    assert await delete_task is True
    assert await store.get_threads() == []


@pytest.mark.asyncio
async def test_delete_thread_ignores_unrelated_buffered_runs():
    store = mem_store(event_flush_interval=10)
    await store.initialize()
    try:
        await store.put_run("thread-a", "run-a", parent_run_id=None, run_input={"text": "test"})
        await store.put_run("thread-b", "run-b", parent_run_id=None, run_input={"text": "test"})
        await store.put_event("run-a", "thread-a", 0, "RUN_STARTED", {"threadId": "thread-a"})
        await store.put_event("missing-run", "thread-b", 0, "RUN_STARTED", {"threadId": "thread-b"})

        assert await store.delete_thread("thread-a") is True

        threads = await store.get_threads()
        assert [thread.thread_id for thread in threads] == ["thread-b"]
    finally:
        try:
            await store.close()
        except Exception:
            pass


@pytest.mark.asyncio
async def test_close_does_not_dispose_caller_provided_engine():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    store = AGUIPersistence(PersistenceConfig(engine=engine, enable_event_buffering=False))
    await store.initialize()
    await store.close()

    async with engine.connect() as conn:
        result = await conn.execute(text("SELECT COUNT(*) FROM agui_threads"))
        assert result.scalar() == 0

    await engine.dispose()


@pytest.mark.asyncio
async def test_flusher_task_stops_when_buffer_becomes_idle():
    store = mem_store(persist_on_completion=False)
    await store.initialize()
    try:
        await store.put_run("thread-1", "run-1", parent_run_id=None, run_input={"text": "test"})
        await store.put_event("run-1", "thread-1", 0, "RUN_STARTED", {"threadId": "thread-1"})
        await store.get_events("run-1")

        for _ in range(10):
            if store._flusher_task is None:
                break
            await asyncio.sleep(0)

        assert store._flusher_task is None
    finally:
        await store.close()


@pytest.mark.asyncio
async def test_enqueue_during_idle_flusher_exit_starts_replacement_flusher():
    store = mem_store(event_flush_interval=10, persist_on_completion=False)
    await store.initialize()
    try:
        await store.put_run("thread-1", "run-1", parent_run_id=None, run_input={"text": "test"})
        await store.put_event("run-1", "thread-1", 0, "RUN_STARTED", {"threadId": "thread-1"})
        await store.get_events("run-1")

        old_task = asyncio.create_task(store._event_flusher_loop())
        store._flusher_task = old_task

        await asyncio.sleep(0)
        await store.put_event("run-1", "thread-1", 1, "TEXT_MESSAGE_END", {"threadId": "thread-1"})
        events = await store.get_events("run-1")

        assert [event.seq for event in events] == [0, 1]
    finally:
        await store.close()


@pytest.mark.asyncio
async def test_get_threads_before_cursor(store):
    import asyncio
    await store.put_run("thread-1", "run-1", parent_run_id=None, run_input={"text": "test"})
    await asyncio.sleep(0.01)
    await store.put_run("thread-2", "run-2", parent_run_id=None, run_input={"text": "test"})
    threads = await store.get_threads(limit=10)
    assert threads[0].thread_id == "thread-2"  # most recent first

    older = await store.get_threads(limit=10, before="thread-2")
    assert len(older) == 1
    assert older[0].thread_id == "thread-1"


@pytest.mark.asyncio
async def test_run_seq_increments(store):
    await store.put_run("thread-1", "run-1", parent_run_id=None, run_input={"text": "test"})
    await store.put_run("thread-1", "run-2", parent_run_id=None, run_input={"text": "test"})
    runs = await store.get_runs("thread-1")
    seqs = {r.run_id: r.seq for r in runs}
    assert seqs["run-1"] == 0
    assert seqs["run-2"] == 1


@pytest.mark.asyncio
async def test_delete_thread_respects_namespace(store):
    await store.put_run("thread-1", "run-1", parent_run_id=None, namespace="project-a", run_input={"text": "test"})

    deleted = await store.delete_thread("thread-1", namespace="project-b")
    assert deleted is False
    assert len(await store.get_threads()) == 1

    deleted = await store.delete_thread("thread-1", namespace="project-a")
    assert deleted is True
    assert await store.get_threads() == []


@pytest.mark.asyncio
async def test_missing_db_url_and_engine_raises():
    with pytest.raises(ValueError, match="db_url.*engine"):
        AGUIPersistence(PersistenceConfig())


# ------------------------------------------------------------------
# persist_on_completion tests
# ------------------------------------------------------------------

@pytest.mark.asyncio
async def test_persist_on_completion_get_events_returns_buffered_events():
    store = mem_store(persist_on_completion=True)
    await store.initialize()
    try:
        await store.put_run("thread-1", "run-1", parent_run_id=None, run_input={"text": "test"})
        await store.put_event("run-1", "thread-1", 0, "RUN_STARTED", {"threadId": "thread-1"})
        await store.put_event("run-1", "thread-1", 1, "TEXT_MESSAGE_START", {"messageId": "msg-1"})

        # Events are buffered — DB should be empty
        async with store._engine.connect() as conn:
            result = await conn.execute(
                text("SELECT COUNT(*) FROM agui_events WHERE run_id = :rid"),
                {"rid": "run-1"},
            )
            assert result.scalar() == 0

        # get_events should return the in-memory buffered events without flushing them
        events = await store.get_events("run-1")
        assert [event.seq for event in events] == [0, 1]
        assert events[0].event_type == "RUN_STARTED"
        assert events[1].event_type == "TEXT_MESSAGE_START"

        # DB still empty after get_events
        async with store._engine.connect() as conn:
            result = await conn.execute(
                text("SELECT COUNT(*) FROM agui_events WHERE run_id = :rid"),
                {"rid": "run-1"},
            )
            assert result.scalar() == 0

        # Completing the run triggers flush
        await store.update_run("run-1", status="completed")

        async with store._engine.connect() as conn:
            result = await conn.execute(
                text("SELECT COUNT(*) FROM agui_events WHERE run_id = :rid"),
                {"rid": "run-1"},
            )
            assert result.scalar() == 2

        # Now get_events reads from DB
        events = await store.get_events("run-1")
        assert len(events) == 2
        assert events[0].event_type == "RUN_STARTED"
    finally:
        await store.close()


@pytest.mark.asyncio
async def test_persist_on_completion_get_events_merges_buffered_deltas():
    store = mem_store(persist_on_completion=True, merge_delta_events=True)
    await store.initialize()
    try:
        await store.put_run("thread-1", "run-1", parent_run_id=None, run_input={"text": "test"})
        await store.put_event("run-1", "thread-1", 0, "TEXT_MESSAGE_CONTENT", {"messageId": "msg-1", "delta": "Hello"})
        await store.put_event("run-1", "thread-1", 1, "TEXT_MESSAGE_CONTENT", {"messageId": "msg-1", "delta": ", "})
        await store.put_event("run-1", "thread-1", 2, "TEXT_MESSAGE_CONTENT", {"messageId": "msg-1", "delta": "world"})
        await store.put_event("run-1", "thread-1", 3, "TEXT_MESSAGE_END", {"messageId": "msg-1"})

        events = await store.get_events("run-1")

        assert len(events) == 2
        assert events[0].event_type == "TEXT_MESSAGE_CONTENT"
        assert events[0].seq == 0
        assert events[0].data["delta"] == "Hello, world"
        assert events[1].event_type == "TEXT_MESSAGE_END"
    finally:
        await store.close()


def _stall_engine_begin(store):
    """Wrap store._engine so the first begin() stalls until the returned release event is set.

    Returns (started, release): started is set when the DB transaction has been entered;
    release must be set by the caller to let it proceed.
    """
    started = asyncio.Event()
    release = asyncio.Event()
    original_begin = store._engine.begin

    class ControlledBegin:
        def __init__(self, cm):
            self._cm = cm

        async def __aenter__(self_inner):
            conn = await self_inner._cm.__aenter__()
            started.set()
            await release.wait()
            return conn

        async def __aexit__(self_inner, exc_type, exc, tb):
            return await self_inner._cm.__aexit__(exc_type, exc, tb)

    class EngineProxy:
        def __init__(self, engine):
            self._engine = engine

        def begin(self):
            return ControlledBegin(original_begin())

        def __getattr__(self, name):
            return getattr(self._engine, name)

    store._engine = EngineProxy(store._engine)
    return started, release


@pytest.mark.asyncio
async def test_persist_on_completion_get_events_waits_for_same_run_atomic_finalize():
    store = mem_store(persist_on_completion=True)
    await store.initialize()
    try:
        await store.put_run("thread-1", "run-1", parent_run_id=None, run_input={"text": "test"})
        await store.put_event("run-1", "thread-1", 0, "RUN_STARTED", {"threadId": "thread-1"})

        started, release = _stall_engine_begin(store)

        update_task = asyncio.create_task(store.update_run("run-1", status="completed"))
        await started.wait()

        read_task = asyncio.create_task(store.get_events("run-1"))
        await asyncio.sleep(0)
        assert not read_task.done()

        release.set()
        await update_task
        events = await read_task

        assert [event.seq for event in events] == [0]
        assert events[0].event_type == "RUN_STARTED"
    finally:
        await store.close()


@pytest.mark.asyncio
async def test_persist_on_completion_get_events_waits_for_inflight_background_flush():
    """get_events must not return a snapshot with a gap when a background flush is in-flight.

    Regression test for the bug where pending_events being empty (events moved to an
    in-flight batch by the background flusher) caused get_events to return only DB events
    before the batch committed, dropping the newest events entirely.
    """
    store = mem_store(persist_on_completion=True)
    await store.initialize()
    try:
        await store.put_run("thread-1", "run-1", parent_run_id=None, run_input={"text": "test"})
        await store.put_event("run-1", "thread-1", 0, "RUN_STARTED", {"threadId": "thread-1"})
        await store.put_event("run-1", "thread-1", 1, "TEXT_MESSAGE_START", {"messageId": "msg-1"})

        started, release = _stall_engine_begin(store)

        # _flush_run marks flush_requested=True, starts the background flusher, and
        # waits for commit.  The flusher will move events out of pending_events into the
        # in-flight batch and then pause inside begin() before committing.
        flush_task = asyncio.create_task(store._flush_run("run-1"))
        await started.wait()  # flusher has taken events; pending_events is now empty

        # At this point: inflight_event_token > 0, pending_events == [], DB still empty.
        # get_events must wait for the flush rather than returning stale/empty history.
        read_task = asyncio.create_task(store.get_events("run-1"))
        await asyncio.sleep(0)
        assert not read_task.done()

        release.set()
        await flush_task
        events = await read_task

        assert [event.seq for event in events] == [0, 1]
        assert events[0].event_type == "RUN_STARTED"
        assert events[1].event_type == "TEXT_MESSAGE_START"
    finally:
        await store.close()


@pytest.mark.asyncio
async def test_persist_on_completion_flushed_on_error_status():
    store = mem_store(persist_on_completion=True)
    await store.initialize()
    try:
        await store.put_run("thread-1", "run-1", parent_run_id=None, run_input={"text": "test"})
        await store.put_event("run-1", "thread-1", 0, "RUN_STARTED", {"threadId": "thread-1"})

        # error is also a terminal status
        await store.update_run("run-1", status="error")

        async with store._engine.connect() as conn:
            result = await conn.execute(
                text("SELECT COUNT(*) FROM agui_events WHERE run_id = :rid"),
                {"rid": "run-1"},
            )
            assert result.scalar() == 1
    finally:
        await store.close()


@pytest.mark.asyncio
async def test_persist_on_completion_close_discards_unfinished_runs(tmp_path):
    db_path = tmp_path / "poc-close.db"
    store = AGUIPersistence(PersistenceConfig(db_url=f"sqlite:///{db_path}", persist_on_completion=True))
    await store.initialize()
    await store.put_run("thread-1", "run-finished", parent_run_id=None, run_input={"text": "test"})
    await store.put_run("thread-1", "run-partial", parent_run_id=None, run_input={"text": "test"})
    await store.put_event("run-finished", "thread-1", 0, "RUN_STARTED", {})
    await store.put_event("run-partial", "thread-1", 0, "RUN_STARTED", {})
    await store.update_run("run-finished", status="completed")
    # run-partial never reaches terminal status — close() should discard it
    await store.close()

    async with store._engine.connect() as conn:
        finished = await conn.execute(
            text("SELECT COUNT(*) FROM agui_events WHERE run_id = :rid"),
            {"rid": "run-finished"},
        )
        partial = await conn.execute(
            text("SELECT COUNT(*) FROM agui_events WHERE run_id = :rid"),
            {"rid": "run-partial"},
        )
        assert finished.scalar() == 1
        assert partial.scalar() == 0


@pytest.mark.asyncio
async def test_persist_on_completion_non_terminal_update_does_not_flush():
    store = mem_store(persist_on_completion=True)
    await store.initialize()
    try:
        await store.put_run("thread-1", "run-1", parent_run_id=None, run_input={"text": "test"})
        await store.put_event("run-1", "thread-1", 0, "RUN_STARTED", {"threadId": "thread-1"})

        # Non-terminal status update — should not flush
        await store.update_run("run-1", status="running")

        async with store._engine.connect() as conn:
            result = await conn.execute(
                text("SELECT COUNT(*) FROM agui_events WHERE run_id = :rid"),
                {"rid": "run-1"},
            )
            assert result.scalar() == 0
    finally:
        await store.close()


@pytest.mark.asyncio
async def test_persist_on_completion_batch_size_does_not_trigger_early_flush():
    """Exceeding event_batch_size must not cause a partial write when persist_on_completion=True."""
    store = mem_store(persist_on_completion=True, event_batch_size=2)
    await store.initialize()
    try:
        await store.put_run("thread-1", "run-1", parent_run_id=None, run_input={"text": "test"})
        await store.put_event("run-1", "thread-1", 0, "RUN_STARTED", {"threadId": "thread-1"})
        await store.put_event("run-1", "thread-1", 1, "TEXT_MESSAGE_START", {"messageId": "m1"})
        # Third event exceeds event_batch_size=2 — must NOT flush early
        await store.put_event("run-1", "thread-1", 2, "TEXT_MESSAGE_END", {"messageId": "m1"})

        async with store._engine.connect() as conn:
            count = (await conn.execute(
                text("SELECT COUNT(*) FROM agui_events WHERE run_id = :rid"), {"rid": "run-1"},
            )).scalar()
            assert count == 0

        await store.update_run("run-1", status="completed")

        async with store._engine.connect() as conn:
            count = (await conn.execute(
                text("SELECT COUNT(*) FROM agui_events WHERE run_id = :rid"), {"rid": "run-1"},
            )).scalar()
            assert count == 3
    finally:
        await store.close()


@pytest.mark.asyncio
async def test_persist_on_completion_events_and_status_written_atomically():
    """Events and run status must be visible together — never events without terminal status."""
    store = mem_store(persist_on_completion=True)
    await store.initialize()
    try:
        await store.put_run("thread-1", "run-1", parent_run_id=None, run_input={"text": "test"})
        await store.put_event("run-1", "thread-1", 0, "RUN_STARTED", {"threadId": "thread-1"})
        await store.put_event("run-1", "thread-1", 1, "TEXT_MESSAGE_CONTENT", {"messageId": "m1", "delta": "hi"})

        await store.update_run("run-1", status="completed", summary="done")

        # Both events and terminal status must be visible in the same read
        async with store._engine.connect() as conn:
            event_count = (await conn.execute(
                text("SELECT COUNT(*) FROM agui_events WHERE run_id = :rid"), {"rid": "run-1"},
            )).scalar()
            run_row = (await conn.execute(
                text("SELECT status FROM agui_runs WHERE run_id = :rid"), {"rid": "run-1"},
            )).fetchone()

        assert event_count == 2
        assert run_row is not None and run_row[0] == "completed"
    finally:
        await store.close()


# ------------------------------------------------------------------
# merge_delta_events tests
# ------------------------------------------------------------------

@pytest.mark.asyncio
async def test_merge_delta_events_text_message_content():
    store = mem_store(merge_delta_events=True, persist_on_completion=False)
    await store.initialize()
    try:
        await store.put_run("thread-1", "run-1", parent_run_id=None, run_input={"text": "test"})
        await store.put_event("run-1", "thread-1", 0, "RUN_STARTED", {"threadId": "thread-1"})
        await store.put_event("run-1", "thread-1", 1, "TEXT_MESSAGE_CONTENT", {"messageId": "msg-1", "delta": "Hello"})
        await store.put_event("run-1", "thread-1", 2, "TEXT_MESSAGE_CONTENT", {"messageId": "msg-1", "delta": ", "})
        await store.put_event("run-1", "thread-1", 3, "TEXT_MESSAGE_CONTENT", {"messageId": "msg-1", "delta": "world"})
        await store.put_event("run-1", "thread-1", 4, "TEXT_MESSAGE_END", {"messageId": "msg-1"})

        events = await store.get_events("run-1")

        # 3 deltas collapsed to 1, so total = RUN_STARTED + merged_delta + TEXT_MESSAGE_END = 3
        assert len(events) == 3
        assert events[0].event_type == "RUN_STARTED"
        assert events[1].event_type == "TEXT_MESSAGE_CONTENT"
        assert events[1].seq == 1  # keeps seq of first delta
        assert events[1].data["delta"] == "Hello, world"
        assert events[1].data["messageId"] == "msg-1"
        assert events[2].event_type == "TEXT_MESSAGE_END"
    finally:
        await store.close()


@pytest.mark.asyncio
async def test_merge_delta_events_tool_call_args():
    store = mem_store(merge_delta_events=True, persist_on_completion=False)
    await store.initialize()
    try:
        await store.put_run("thread-1", "run-1", parent_run_id=None, run_input={"text": "test"})
        await store.put_event("run-1", "thread-1", 0, "TOOL_CALL_ARGS", {"toolCallId": "tc-1", "delta": '{"key"'})
        await store.put_event("run-1", "thread-1", 1, "TOOL_CALL_ARGS", {"toolCallId": "tc-1", "delta": ': "val'})
        await store.put_event("run-1", "thread-1", 2, "TOOL_CALL_ARGS", {"toolCallId": "tc-1", "delta": 'ue"}'})

        events = await store.get_events("run-1")

        assert len(events) == 1
        assert events[0].event_type == "TOOL_CALL_ARGS"
        assert events[0].seq == 0
        assert events[0].data["delta"] == '{"key": "value"}'
        assert events[0].data["toolCallId"] == "tc-1"
    finally:
        await store.close()


@pytest.mark.asyncio
async def test_merge_delta_events_different_message_ids_not_merged():
    store = mem_store(merge_delta_events=True, persist_on_completion=False)
    await store.initialize()
    try:
        await store.put_run("thread-1", "run-1", parent_run_id=None, run_input={"text": "test"})
        await store.put_event("run-1", "thread-1", 0, "TEXT_MESSAGE_CONTENT", {"messageId": "msg-1", "delta": "Hi"})
        await store.put_event("run-1", "thread-1", 1, "TEXT_MESSAGE_CONTENT", {"messageId": "msg-2", "delta": "Bye"})

        events = await store.get_events("run-1")

        # Different messageIds — not merged
        assert len(events) == 2
        assert events[0].data["messageId"] == "msg-1"
        assert events[1].data["messageId"] == "msg-2"
    finally:
        await store.close()


@pytest.mark.asyncio
async def test_merge_delta_events_non_adjacent_same_id_not_merged():
    """A TEXT_MESSAGE_END between two TEXT_MESSAGE_CONTENT deltas for the same message
    must prevent them from being merged — the boundary event breaks adjacency."""
    store = mem_store(merge_delta_events=True, persist_on_completion=False)
    await store.initialize()
    try:
        await store.put_run("thread-1", "run-1", parent_run_id=None, run_input={"text": "test"})
        await store.put_event("run-1", "thread-1", 0, "TEXT_MESSAGE_CONTENT", {"messageId": "msg-1", "delta": "Hello"})
        await store.put_event("run-1", "thread-1", 1, "TEXT_MESSAGE_END", {"messageId": "msg-1"})
        await store.put_event("run-1", "thread-1", 2, "TEXT_MESSAGE_CONTENT", {"messageId": "msg-1", "delta": "World"})

        events = await store.get_events("run-1")

        assert len(events) == 3
        assert events[0].data["delta"] == "Hello"
        assert events[1].event_type == "TEXT_MESSAGE_END"
        assert events[2].data["delta"] == "World"
    finally:
        await store.close()


@pytest.mark.asyncio
async def test_merge_delta_events_across_flush_boundaries():
    """Deltas split across two flush batches must produce a single merged DB row."""
    store = mem_store(merge_delta_events=True, event_flush_interval=10, persist_on_completion=False)
    await store.initialize()
    try:
        await store.put_run("thread-1", "run-1", parent_run_id=None, run_input={"text": "test"})
        await store.put_event("run-1", "thread-1", 0, "TEXT_MESSAGE_CONTENT", {"messageId": "msg-1", "delta": "Hello"})
        await store.put_event("run-1", "thread-1", 1, "TEXT_MESSAGE_CONTENT", {"messageId": "msg-1", "delta": ", "})
        # Force first batch to flush before the next events arrive
        await store._flush_run("run-1")

        async with store._engine.connect() as conn:
            count = (await conn.execute(
                text("SELECT COUNT(*) FROM agui_events WHERE run_id = :rid"), {"rid": "run-1"},
            )).scalar()
            assert count == 1  # two deltas already merged into one row

        # Second batch continues the same delta stream
        await store.put_event("run-1", "thread-1", 2, "TEXT_MESSAGE_CONTENT", {"messageId": "msg-1", "delta": "world"})
        await store.put_event("run-1", "thread-1", 3, "TEXT_MESSAGE_END", {"messageId": "msg-1"})

        events = await store.get_events("run-1")

        assert len(events) == 2  # all three deltas in one row + end
        assert events[0].seq == 0
        assert events[0].data["delta"] == "Hello, world"
        assert events[1].event_type == "TEXT_MESSAGE_END"
    finally:
        await store.close()


@pytest.mark.asyncio
async def test_merge_delta_events_disabled_by_default(store):
    await store.put_run("thread-1", "run-1", parent_run_id=None, run_input={"text": "test"})
    await store.put_event("run-1", "thread-1", 0, "TEXT_MESSAGE_CONTENT", {"messageId": "msg-1", "delta": "Hello"})
    await store.put_event("run-1", "thread-1", 1, "TEXT_MESSAGE_CONTENT", {"messageId": "msg-1", "delta": " world"})

    events = await store.get_events("run-1")
    assert len(events) == 2  # not merged


# ------------------------------------------------------------------
# started_at / ended_at tests
# ------------------------------------------------------------------

@pytest.mark.asyncio
async def test_non_delta_events_have_equal_started_at_ended_at(store):
    await store.put_run("thread-1", "run-1", parent_run_id=None, run_input={"text": "test"})
    await store.put_event("run-1", "thread-1", 0, "RUN_STARTED", {"threadId": "thread-1"})
    await store.put_event("run-1", "thread-1", 1, "TEXT_MESSAGE_START", {"messageId": "msg-1"})

    events = await store.get_events("run-1")
    for event in events:
        assert event.started_at == event.ended_at
        assert event.started_at > 0


@pytest.mark.asyncio
async def test_merged_event_timestamps_span_first_to_last_delta():
    store = mem_store(merge_delta_events=True, persist_on_completion=False)
    await store.initialize()
    try:
        await store.put_run("thread-1", "run-1", parent_run_id=None, run_input={"text": "test"})
        await store.put_event("run-1", "thread-1", 0, "TEXT_MESSAGE_CONTENT", {"messageId": "msg-1", "delta": "a"})
        await asyncio.sleep(0.01)
        await store.put_event("run-1", "thread-1", 1, "TEXT_MESSAGE_CONTENT", {"messageId": "msg-1", "delta": "b"})
        await asyncio.sleep(0.01)
        await store.put_event("run-1", "thread-1", 2, "TEXT_MESSAGE_CONTENT", {"messageId": "msg-1", "delta": "c"})

        events = await store.get_events("run-1")
        assert len(events) == 1
        merged = events[0]
        # started_at from first delta, ended_at from last — so ended_at >= started_at
        assert merged.ended_at >= merged.started_at
        assert merged.data["delta"] == "abc"
    finally:
        await store.close()


@pytest.mark.asyncio
async def test_unbuffered_events_have_equal_started_at_ended_at():
    store = mem_store(enable_event_buffering=False, persist_on_completion=False)
    await store.initialize()
    try:
        await store.put_run("thread-1", "run-1", parent_run_id=None, run_input={"text": "test"})
        await store.put_event("run-1", "thread-1", 0, "RUN_STARTED", {"threadId": "thread-1"})

        events = await store.get_events("run-1")
        assert len(events) == 1
        assert events[0].started_at == events[0].ended_at
        assert events[0].started_at > 0
    finally:
        await store.close()


# ------------------------------------------------------------------
# agui_events.thread_id / FK-less events / flush_events
#
# wm-agent-server stops calling put_run/update_run/get_run/get_runs/get_all_runs/
# get_threads entirely (it tracks all run metadata in its own store) and uses this
# library purely for the event stream. agui_events.thread_id lets events be written,
# read, and deleted without ever going through agui_runs — these tests exercise that
# path directly, deliberately never calling put_run for the events under test.
# ------------------------------------------------------------------

@pytest.mark.asyncio
async def test_put_event_writes_thread_id_column():
    store = mem_store(enable_event_buffering=False, persist_on_completion=False)
    await store.initialize()
    try:
        await store.put_event("run-1", "thread-1", 0, "RUN_STARTED", {})

        async with store._engine.connect() as conn:
            row = (await conn.execute(
                text("SELECT thread_id FROM agui_events WHERE run_id = :rid AND seq = 0"),
                {"rid": "run-1"},
            )).fetchone()
            assert row[0] == "thread-1"
    finally:
        await store.close()


@pytest.mark.asyncio
async def test_put_event_succeeds_with_no_corresponding_agui_runs_row():
    """agui_events.run_id has no FK to agui_runs — an event for a run this library
    never tracked (because the caller's own store owns run metadata) must still
    write and read back cleanly."""
    store = mem_store(enable_event_buffering=False, persist_on_completion=False)
    await store.initialize()
    try:
        await store.put_event("untracked-run", "thread-1", 0, "RUN_STARTED", {"foo": "bar"})
        events = await store.get_events("untracked-run")
        assert len(events) == 1
        assert events[0].data == {"foo": "bar"}
    finally:
        await store.close()


@pytest.mark.asyncio
async def test_delete_thread_deletes_events_by_thread_id_without_agui_runs():
    """delete_thread must delete agui_events directly by thread_id, not via a
    sub-select through agui_runs — must work even when agui_runs has zero rows."""
    store = mem_store(event_flush_interval=10)
    await store.initialize()
    try:
        await store.put_run("thread-1", "run-1", parent_run_id=None, run_input={"text": "test"})
        await store.put_event("untracked-run", "thread-1", 0, "RUN_STARTED", {})
        await store._flush_all_events()

        async with store._engine.connect() as conn:
            count = (await conn.execute(
                text("SELECT COUNT(*) FROM agui_events WHERE run_id = :rid"),
                {"rid": "untracked-run"},
            )).scalar()
            assert count == 1  # sanity: event landed despite no agui_runs row for it

        assert await store.delete_thread("thread-1") is True

        async with store._engine.connect() as conn:
            count = (await conn.execute(
                text("SELECT COUNT(*) FROM agui_events WHERE thread_id = :tid"),
                {"tid": "thread-1"},
            )).scalar()
            assert count == 0
    finally:
        await store.close()


@pytest.mark.asyncio
async def test_delete_thread_deletes_events_when_thread_never_tracked_in_agui_threads():
    """A caller that only ever calls put_event (never put_run) — the events-only usage
    this feature exists for — has zero agui_threads rows for its thread_id. delete_thread
    must still delete that thread's agui_events and report True, not silently no-op
    because the agui_threads DELETE matched nothing."""
    store = mem_store(event_flush_interval=10)
    await store.initialize()
    try:
        await store.put_event("run-1", "thread-1", 0, "RUN_STARTED", {})
        await store._flush_all_events()

        async with store._engine.connect() as conn:
            count = (await conn.execute(
                text("SELECT COUNT(*) FROM agui_threads WHERE thread_id = :tid"),
                {"tid": "thread-1"},
            )).scalar()
            assert count == 0  # sanity: never tracked via put_run

        assert await store.delete_thread("thread-1") is True

        async with store._engine.connect() as conn:
            count = (await conn.execute(
                text("SELECT COUNT(*) FROM agui_events WHERE thread_id = :tid"),
                {"tid": "thread-1"},
            )).scalar()
            assert count == 0
    finally:
        await store.close()


@pytest.mark.asyncio
async def test_flush_events_writes_buffered_events_and_never_touches_agui_runs():
    store = mem_store(event_flush_interval=10)
    await store.initialize()
    try:
        await store.put_event("run-1", "thread-1", 0, "RUN_STARTED", {})
        await store.put_event("run-1", "thread-1", 1, "TEXT_MESSAGE_START", {"messageId": "m1"})

        async with store._engine.connect() as conn:
            count = (await conn.execute(
                text("SELECT COUNT(*) FROM agui_events WHERE run_id = :rid"), {"rid": "run-1"},
            )).scalar()
            assert count == 0  # still buffered

        await store.flush_events("run-1")

        async with store._engine.connect() as conn:
            count = (await conn.execute(
                text("SELECT COUNT(*) FROM agui_events WHERE run_id = :rid"), {"rid": "run-1"},
            )).scalar()
            assert count == 2
            # No agui_runs row was ever created for this run — flush_events must not
            # require or create one.
            runs_count = (await conn.execute(
                text("SELECT COUNT(*) FROM agui_runs WHERE run_id = :rid"), {"rid": "run-1"},
            )).scalar()
            assert runs_count == 0
    finally:
        await store.close()


@pytest.mark.asyncio
async def test_flush_events_is_a_noop_when_nothing_buffered():
    store = mem_store()
    await store.initialize()
    try:
        await store.flush_events("never-seen-run")  # must not raise
    finally:
        await store.close()


@pytest.mark.asyncio
async def test_flush_events_waits_for_inflight_batch():
    """A second flush_events call must wait for an already-in-flight background
    batch (triggered by a first, concurrent flush_events call) to commit, rather
    than racing it or returning before the events are actually durable."""
    store = mem_store(event_flush_interval=10)
    await store.initialize()
    try:
        started = asyncio.Event()
        release = asyncio.Event()
        original_flush_batch = store._flush_batch

        async def controlled_flush(batch):
            started.set()
            await release.wait()
            await original_flush_batch(batch)

        store._flush_batch = controlled_flush

        await store.put_event("run-1", "thread-1", 0, "RUN_STARTED", {})

        first_flush = asyncio.create_task(store.flush_events("run-1"))
        await started.wait()  # background flusher has taken ownership of the batch

        second_flush = asyncio.create_task(store.flush_events("run-1"))
        await asyncio.sleep(0)
        assert not first_flush.done()
        assert not second_flush.done()

        release.set()
        await first_flush
        await second_flush

        async with store._engine.connect() as conn:
            count = (await conn.execute(
                text("SELECT COUNT(*) FROM agui_events WHERE run_id = :rid"), {"rid": "run-1"},
            )).scalar()
            assert count == 1
    finally:
        await store.close()


@pytest.mark.asyncio
async def test_delete_run_deletes_events_and_agui_runs_row():
    store = mem_store(event_flush_interval=10)
    await store.initialize()
    try:
        await store.put_run("thread-1", "run-1", parent_run_id=None, run_input={"text": "test"})
        await store.put_event("run-1", "thread-1", 0, "RUN_STARTED", {})
        await store.flush_events("run-1")

        assert await store.delete_run("run-1") is True

        async with store._engine.connect() as conn:
            events_count = (await conn.execute(
                text("SELECT COUNT(*) FROM agui_events WHERE run_id = :rid"), {"rid": "run-1"},
            )).scalar()
            runs_count = (await conn.execute(
                text("SELECT COUNT(*) FROM agui_runs WHERE run_id = :rid"), {"rid": "run-1"},
            )).scalar()
            assert events_count == 0
            assert runs_count == 0
    finally:
        await store.close()


@pytest.mark.asyncio
async def test_delete_run_flushes_buffered_events_before_deleting():
    """A run with events still sitting in the in-memory buffer (never explicitly flushed)
    must have them flushed then deleted — not left to land in agui_events later via the
    background flusher, after delete_run already returned."""
    store = mem_store(event_flush_interval=10)
    await store.initialize()
    try:
        await store.put_event("run-1", "thread-1", 0, "RUN_STARTED", {})

        async with store._engine.connect() as conn:
            count = (await conn.execute(
                text("SELECT COUNT(*) FROM agui_events WHERE run_id = :rid"), {"rid": "run-1"},
            )).scalar()
            assert count == 0  # still buffered, sanity check

        assert await store.delete_run("run-1") is True

        async with store._engine.connect() as conn:
            count = (await conn.execute(
                text("SELECT COUNT(*) FROM agui_events WHERE run_id = :rid"), {"rid": "run-1"},
            )).scalar()
            assert count == 0  # flushed then deleted, not left to reappear later
    finally:
        await store.close()


@pytest.mark.asyncio
async def test_delete_run_returns_false_when_nothing_matches():
    store = mem_store()
    await store.initialize()
    try:
        assert await store.delete_run("never-seen-run") is False
    finally:
        await store.close()


@pytest.mark.asyncio
async def test_delete_run_does_not_affect_other_runs():
    store = mem_store(event_flush_interval=10)
    await store.initialize()
    try:
        await store.put_event("run-1", "thread-1", 0, "RUN_STARTED", {})
        await store.put_event("run-2", "thread-1", 0, "RUN_STARTED", {})
        await store.flush_events("run-1")
        await store.flush_events("run-2")

        await store.delete_run("run-1")

        async with store._engine.connect() as conn:
            remaining = (await conn.execute(
                text("SELECT COUNT(*) FROM agui_events WHERE run_id = :rid"), {"rid": "run-2"},
            )).scalar()
            assert remaining == 1
    finally:
        await store.close()


@pytest.mark.asyncio
async def test_delete_run_does_not_touch_agui_threads_row():
    """delete_run must never delete/alter the thread row a run belongs to — a thread holds
    many runs, so deleting one run's data must leave agui_threads untouched."""
    store = mem_store(event_flush_interval=10)
    await store.initialize()
    try:
        await store.put_run("thread-1", "run-1", parent_run_id=None, run_input={"text": "test"})

        await store.delete_run("run-1")

        async with store._engine.connect() as conn:
            count = (await conn.execute(
                text("SELECT COUNT(*) FROM agui_threads WHERE thread_id = :tid"), {"tid": "thread-1"},
            )).scalar()
            assert count == 1
    finally:
        await store.close()
