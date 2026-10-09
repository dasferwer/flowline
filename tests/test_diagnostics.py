import uuid

from test_engine import register, start

from flowline.db import connect
from flowline.engine import claim, finish


def diagnostics(client, identity):
    response = client.get(f"/runs/{identity}/diagnostics")
    assert response.status_code == 200, response.text
    return {node["name"]: node for node in response.json()["nodes"]}


def test_dependencies_running_lease_and_ready_are_distinct(client):
    register(client)
    identity = start(client)
    before = client.get(f"/runs/{identity}").json()
    result = diagnostics(client, identity)
    assert result["a"]["reasons"] == ["ready"]
    assert result["b"]["reasons"] == ["dependencies"]
    assert result["b"]["blocking_dependencies"] == ["a"]
    assert client.get(f"/runs/{identity}").json() == before
    node = claim()
    result = diagnostics(client, identity)
    assert result["a"]["reasons"] == ["running"]
    assert result["a"]["lease_until"] is not None
    with connect() as conn:
        conn.execute(
            "UPDATE nodes SET lease_until=now()-interval '1 second' WHERE run_id=%s AND name='a'",
            (identity,),
        )
    assert diagnostics(client, identity)["a"]["reasons"] == ["lease_expired"]
    assert not finish(node, 9)


def test_run_parallelism_and_retry_delay_are_explained(client):
    register(
        client,
        parallelism=1,
        nodes=[{"name": "a", "task": "constant"}, {"name": "b", "task": "constant"}],
    )
    identity = start(client)
    node = claim()
    assert diagnostics(client, identity)["b"]["reasons"] == ["run_limit"]
    finish(node, error="failure")
    assert "retry_delay" in diagnostics(client, identity)["a"]["reasons"]
    with connect() as conn:
        conn.execute(
            "UPDATE nodes SET status='failed',attempts=3 WHERE run_id=%s AND name='a'", (identity,)
        )
        conn.execute("UPDATE runs SET status='failed' WHERE id=%s", (identity,))
    result = diagnostics(client, identity)
    assert result["a"]["reasons"] == ["retry_exhausted"]
    assert result["b"]["reasons"] == ["run_stopped"]


def test_global_limit_and_reserved_current_slot(client):
    register(
        client, parallelism=4, nodes=[{"name": chr(97 + i), "task": "constant"} for i in range(4)]
    )
    response = client.post(
        "/backfills", json={"name": "demo", "version": 1, "logical_date": "2025-01-01", "days": 1}
    )
    backfill = response.json()["ids"][0]
    for _ in range(3):
        assert claim()
    assert diagnostics(client, backfill)["d"]["reasons"] == ["backfill_limit"]
    identity = start(client)
    assert claim()
    assert "global_limit" in diagnostics(client, identity)["b"]["reasons"]


def test_missing_and_unauthorized_diagnostics(client):
    assert client.get(f"/runs/{uuid.uuid4()}/diagnostics").status_code == 404
    assert (
        client.get(f"/runs/{uuid.uuid4()}/diagnostics", headers={"X-API-Key": "wrong"}).status_code
        == 401
    )
