import json
import os
import subprocess
import time
import uuid

import httpx

from flowline.db import connect

base = os.environ.get("API_URL", "http://localhost:8094")


def docker(*arguments):
    subprocess.run(["docker", "compose", *arguments], check=True, capture_output=True)


def wait(client, identity, predicate):
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline:
        state = client.get(f"/runs/{identity}").json()
        if predicate(state):
            return state
        if state["status"] in {"failed", "cancelled"}:
            raise RuntimeError(state)
        time.sleep(0.1)
    raise TimeoutError("Граф не достиг ожидаемого состояния")


with httpx.Client(base_url=base, headers={"X-API-Key": "local-demo-key"}, timeout=30) as client:
    name = "demo-" + uuid.uuid4().hex[:8]
    response = client.post(
        "/definitions",
        json={
            "name": name,
            "version": 1,
            "nodes": [
                {"name": "seed", "task": "constant", "value": 2},
                {"name": "total", "task": "sum", "value": 3, "delay": 4, "dependencies": ["seed"]},
            ],
        },
    )
    response.raise_for_status()
    docker("stop", "worker")
    backfills = []
    try:
        response = client.post(
            "/backfills",
            json={"name": name, "version": 1, "logical_date": "2025-01-01", "days": 30},
        )
        response.raise_for_status()
        backfills = response.json()["ids"]
        response = client.post(
            "/runs", json={"name": name, "version": 1, "logical_date": "2025-02-01"}
        )
        response.raise_for_status()
        identity = response.json()["id"]
        docker("start", "worker")
        wait(
            client,
            identity,
            lambda state: any(
                n["name"] == "total" and n["status"] == "running" for n in state["nodes"]
            ),
        )
        docker("kill", "-s", "SIGKILL", "worker")
        docker("start", "worker")
        result = wait(client, identity, lambda state: state["status"] == "completed")
        total = next(n for n in result["nodes"] if n["name"] == "total")
        assert total["output"] == 5
        assert total["attempts"] >= 2
        with connect() as conn:
            effects = conn.execute(
                "SELECT count(*) AS n FROM effects WHERE run_id=%s", (identity,)
            ).fetchone()["n"]
            pending = conn.execute(
                "SELECT count(*) AS n FROM runs WHERE name=%s AND kind='backfill' AND status='running'",
                (name,),
            ).fetchone()["n"]
        assert effects == 2
        assert pending > 0
        print(
            json.dumps(
                {
                    "output": 5,
                    "effects": effects,
                    "attempts": total["attempts"],
                    "backfills_still_running": pending,
                },
                indent=2,
            )
        )
    finally:
        for identity in backfills:
            client.post(f"/runs/{identity}/cancel")
        docker("start", "worker")
