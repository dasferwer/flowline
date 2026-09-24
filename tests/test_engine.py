from concurrent.futures import ThreadPoolExecutor
from datetime import date

from flowline.db import connect
from flowline.engine import claim, finish
from flowline.scheduler import tick
from flowline.worker import execute


def register(client, name="demo", daily=False, nodes=None, parallelism=2):
    data = {
        "name": name,
        "version": 1,
        "daily": daily,
        "parallelism": parallelism,
        "nodes": nodes
        or [
            {"name": "a", "task": "constant", "value": 2},
            {"name": "b", "task": "sum", "value": 3, "dependencies": ["a"]},
        ],
    }
    assert client.post("/definitions", json=data).status_code == 200
    return data


def start(client, name="demo"):
    response = client.post("/runs", json={"name": name, "version": 1, "logical_date": "2025-02-01"})
    assert response.status_code == 200
    return response.json()["id"]


def test_dag_dependencies_and_exactly_once_database_effect(client):
    register(client)
    identity = start(client)
    first = claim()
    assert first["name"] == "a"
    assert claim() is None
    assert finish(first, execute(first))
    assert not finish(first, 999)
    second = claim()
    assert second["inputs"] == [2]
    assert finish(second, execute(second))
    result = client.get(f"/runs/{identity}").json()
    assert result["status"] == "completed"
    assert result["nodes"][1]["output"] == 5
    with connect() as conn:
        assert conn.execute("SELECT count(*) AS n FROM effects").fetchone()["n"] == 2


def test_expired_worker_cannot_commit(client):
    register(client)
    start(client)
    stale = claim()
    with connect() as conn:
        conn.execute(
            "UPDATE nodes SET lease_until=now()-interval '1 second' WHERE status='running'"
        )
    assert not finish(stale, 100)
    fresh = claim()
    assert fresh["token"] != stale["token"]
    assert not finish(stale, 100)
    assert finish(fresh, 2)


def test_multiple_schedulers_create_one_daily_run(client):
    register(client, daily=True)
    with ThreadPoolExecutor(3) as pool:
        results = list(pool.map(lambda _: tick(date(2025, 2, 1)), range(3)))
    assert results[0] == results[1] == results[2]
    with connect() as conn:
        assert conn.execute("SELECT count(*) AS n FROM runs").fetchone()["n"] == 1
        assert conn.execute("SELECT count(*) AS n FROM nodes").fetchone()["n"] == 2


def test_backfill_cannot_take_current_reserved_slot(client):
    register(client, nodes=[{"name": "a", "task": "constant", "value": 1}])
    response = client.post(
        "/backfills", json={"name": "demo", "version": 1, "logical_date": "2025-01-01", "days": 30}
    )
    assert len(response.json()["ids"]) == 30
    batches = [claim() for _ in range(3)]
    assert all(node["kind"] == "backfill" for node in batches)
    assert claim() is None
    current = start(client)
    assert str(claim()["run_id"]) == current
    assert claim() is None


def test_retry_exhaustion(client):
    register(client)
    identity = start(client)
    for _ in range(3):
        node = claim()
        assert finish(node, error="Ошибка задачи")
        with connect() as conn:
            conn.execute("UPDATE nodes SET available_at=now()-interval '1 second'")
    assert client.get(f"/runs/{identity}").json()["status"] == "failed"
    assert claim() is None


def test_cancel_discards_late_result(client):
    register(client)
    identity = start(client)
    node = claim()
    assert client.post(f"/runs/{identity}/cancel").json()["status"] == "cancelled"
    assert not finish(node, 2)
    assert claim() is None


def test_validation_and_definition_immutability(client):
    original = register(client)
    changed = {**original, "parallelism": 3}
    assert client.post("/definitions", json=changed).status_code == 409
    cyclic = {
        **original,
        "name": "cycle",
        "nodes": [{"name": "a", "task": "sum", "dependencies": ["a"]}],
    }
    assert client.post("/definitions", json=cyclic).status_code == 422
    missing = {**cyclic, "nodes": [{"name": "a", "task": "sum", "dependencies": ["unknown"]}]}
    assert client.post("/definitions", json=missing).status_code == 422
    assert start(client) == start(client)


def test_parallelism_and_concurrent_workers(client):
    register(
        client,
        parallelism=1,
        nodes=[{"name": "a", "task": "constant"}, {"name": "b", "task": "constant"}],
    )
    start(client)
    with ThreadPoolExecutor(3) as pool:
        claimed = list(pool.map(lambda _: claim(), range(3)))
    assert sum(node is not None for node in claimed) == 1


def test_latest_version_disables_daily_schedule(client):
    graph = register(client, daily=True)
    assert (
        client.post("/definitions", json={**graph, "version": 2, "daily": False}).status_code == 200
    )
    assert tick(date(2025, 2, 1)) == []
