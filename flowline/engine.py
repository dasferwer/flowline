import uuid
from graphlib import CycleError, TopologicalSorter

from psycopg.types.json import Jsonb

from flowline.db import connect


def validate(graph):
    names = [node["name"] for node in graph["nodes"]]
    if len(set(names)) != len(names):
        raise ValueError("Имена узлов должны быть уникальны")
    for node in graph["nodes"]:
        if any(dep not in names for dep in node["dependencies"]):
            raise ValueError("Неизвестная зависимость")
    try:
        list(
            TopologicalSorter(
                {node["name"]: node["dependencies"] for node in graph["nodes"]}
            ).static_order()
        )
    except CycleError as exc:
        raise ValueError("Граф содержит цикл") from exc


def create(conn, definition, logical_date, kind):
    identity = uuid.uuid4()
    inserted = conn.execute(
        """INSERT INTO runs (id,name,version,logical_date,kind,parallelism)
        VALUES (%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING RETURNING id""",
        (
            identity,
            definition["name"],
            definition["version"],
            logical_date,
            kind,
            definition["graph"]["parallelism"],
        ),
    ).fetchone()
    if not inserted:
        return conn.execute(
            "SELECT id FROM runs WHERE name=%s AND version=%s AND logical_date=%s AND kind=%s",
            (definition["name"], definition["version"], logical_date, kind),
        ).fetchone()["id"]
    for node in definition["graph"]["nodes"]:
        conn.execute(
            "INSERT INTO nodes (run_id,name,task,value,delay,dependencies) VALUES (%s,%s,%s,%s,%s,%s)",
            (
                identity,
                node["name"],
                node["task"],
                node["value"],
                node["delay"],
                node["dependencies"],
            ),
        )
    return identity


def claim():
    with connect() as conn:
        conn.execute("SELECT pg_advisory_xact_lock(340028)")
        conn.execute("""UPDATE attempts a SET status='lost',finished_at=clock_timestamp(),error='Истёк срок владения'
            FROM nodes n WHERE a.token=n.token AND n.status='running' AND n.lease_until<clock_timestamp()""")
        conn.execute("""UPDATE nodes SET status=CASE WHEN attempts>=3 THEN 'failed' ELSE 'pending' END,
            token=NULL,error='Истёк срок владения' WHERE status='running' AND lease_until<now()""")
        conn.execute("""UPDATE runs SET status='failed' WHERE status='running' AND EXISTS
            (SELECT 1 FROM nodes WHERE run_id=runs.id AND status='failed')""")
        active = conn.execute(
            "SELECT count(*) AS n FROM nodes n JOIN runs r ON r.id=n.run_id WHERE n.status='running' AND r.status='running'"
        ).fetchone()["n"]
        if active >= 4:
            return None
        turn = conn.execute("SELECT turn FROM queue_state WHERE id=1 FOR UPDATE").fetchone()["turn"]
        preferred = "current" if turn % 2 == 0 else "backfill"
        node = conn.execute(
            """SELECT n.*,r.kind FROM nodes n JOIN runs r ON r.id=n.run_id
            WHERE r.status='running' AND n.status='pending' AND n.available_at<=now()
            AND (SELECT count(*) FROM nodes busy WHERE busy.run_id=n.run_id AND busy.status='running')<r.parallelism
            AND NOT EXISTS (SELECT 1 FROM nodes dependency WHERE dependency.run_id=n.run_id
                AND dependency.name=ANY(n.dependencies) AND dependency.status!='completed')
            AND (r.kind='current' OR (SELECT count(*) FROM nodes bn JOIN runs br ON br.id=bn.run_id
                WHERE bn.status='running' AND br.status='running' AND br.kind='backfill')<3)
            ORDER BY (r.kind=%s) DESC,r.created_at,n.name LIMIT 1 FOR UPDATE OF n""",
            (preferred,),
        ).fetchone()
        if not node:
            return None
        token = uuid.uuid4()
        conn.execute(
            """UPDATE nodes SET status='running',attempts=attempts+1,token=%s,
            lease_until=clock_timestamp()+interval '15 seconds',started_at=clock_timestamp() WHERE run_id=%s AND name=%s""",
            (token, node["run_id"], node["name"]),
        )
        conn.execute(
            "INSERT INTO attempts(token,run_id,name,generation) SELECT %s,id,%s,generation FROM runs WHERE id=%s",
            (token, node["name"], node["run_id"]),
        )
        conn.execute("UPDATE queue_state SET turn=turn+1 WHERE id=1")
        dependencies = conn.execute(
            "SELECT output FROM nodes WHERE run_id=%s AND name=ANY(%s)",
            (node["run_id"], node["dependencies"]),
        ).fetchall()
        return {**node, "token": token, "inputs": [item["output"] for item in dependencies]}


def finish(node, value=None, error=None):
    with connect() as conn:
        conn.execute("SELECT pg_advisory_xact_lock(340028)")
        run = conn.execute(
            "SELECT status FROM runs WHERE id=%s FOR UPDATE", (node["run_id"],)
        ).fetchone()
        current = conn.execute(
            "SELECT * FROM nodes WHERE run_id=%s AND name=%s AND token=%s AND status='running' AND lease_until>clock_timestamp() AND started_at>clock_timestamp()-interval '5 minutes' FOR UPDATE",
            (node["run_id"], node["name"], node["token"]),
        ).fetchone()
        if run["status"] != "running" or not current:
            return False
        conn.execute(
            "UPDATE attempts SET status=%s,finished_at=clock_timestamp(),output=%s,error=%s WHERE token=%s",
            (
                "failed" if error else "completed",
                value,
                error[:300] if error else None,
                node["token"],
            ),
        )
        if error is not None:
            status = "failed" if current["attempts"] >= 3 else "pending"
            conn.execute(
                """UPDATE nodes SET status=%s,error=%s,token=NULL,
                available_at=now()+interval '1 second' * attempts WHERE run_id=%s AND name=%s""",
                (status, error[:300], node["run_id"], node["name"]),
            )
            if status == "failed":
                conn.execute("UPDATE runs SET status='failed' WHERE id=%s", (node["run_id"],))
            return True
        # Реестр ограничен эффектами внутри БД: эффект и завершение атомарны.
        conn.execute(
            "INSERT INTO effects VALUES (%s,%s,%s) ON CONFLICT DO NOTHING",
            (node["run_id"], node["name"], value),
        )
        conn.execute(
            "UPDATE nodes SET status='completed',output=%s,error=NULL WHERE run_id=%s AND name=%s",
            (value, node["run_id"], node["name"]),
        )
        conn.execute(
            """UPDATE runs SET status='completed' WHERE id=%s AND NOT EXISTS
            (SELECT 1 FROM nodes WHERE run_id=%s AND status!='completed')""",
            (node["run_id"], node["run_id"]),
        )
        return True


def renew(node):
    with connect() as conn:
        return (
            conn.execute(
                """UPDATE nodes n SET lease_until=clock_timestamp()+interval '15 seconds'
            FROM runs r WHERE r.id=n.run_id AND r.status='running' AND n.run_id=%s AND n.name=%s
            AND n.token=%s AND n.status='running' AND n.lease_until>clock_timestamp()
            AND n.started_at>clock_timestamp()-interval '5 minutes' RETURNING n.name""",
                (node["run_id"], node["name"], node["token"]),
            ).fetchone()
            is not None
        )


def retry(run_id, roots):
    with connect() as conn:
        conn.execute("SELECT pg_advisory_xact_lock(340028)")
        run = conn.execute("SELECT * FROM runs WHERE id=%s FOR UPDATE", (run_id,)).fetchone()
        if run is None or run["status"] == "running":
            raise ValueError("Повтор разрешён только для существующего остановленного запуска")
        nodes = conn.execute(
            "SELECT * FROM nodes WHERE run_id=%s ORDER BY name", (run_id,)
        ).fetchall()
        if not roots or not set(roots) <= {n["name"] for n in nodes}:
            raise ValueError("Укажите существующие корни повторяемых ветвей")
        affected = set(roots)
        while True:
            expanded = affected | {
                n["name"] for n in nodes if affected.intersection(n["dependencies"])
            }
            if expanded == affected:
                break
            affected = expanded
        if any(n["status"] in ("failed", "cancelled") and n["name"] not in affected for n in nodes):
            raise ValueError("Включите все неудачные и отменённые ветви в повтор")
        affected |= {n["name"] for n in nodes if n["status"] in ("pending", "running", "blocked")}
        conn.execute(
            "UPDATE attempts SET status='interrupted',finished_at=clock_timestamp() WHERE run_id=%s AND status='running'",
            (run_id,),
        )
        generation = run["generation"] + 1
        previous = [
            {
                "name": n["name"],
                "status": n["status"],
                "output": n["output"],
                "attempts": n["attempts"],
            }
            for n in nodes
        ]
        conn.execute(
            "INSERT INTO run_events(run_id,generation,roots,previous) VALUES (%s,%s,%s,%s)",
            (run_id, generation, sorted(roots), Jsonb(previous)),
        )
        conn.execute(
            "UPDATE nodes SET status='pending',attempts=0,token=NULL,lease_until=NULL,output=NULL,error=NULL,available_at=clock_timestamp() WHERE run_id=%s AND name=ANY(%s)",
            (run_id, sorted(affected)),
        )
        conn.execute(
            "DELETE FROM effects WHERE run_id=%s AND node=ANY(%s)", (run_id, sorted(affected))
        )
        conn.execute(
            "UPDATE runs SET status='running',generation=%s WHERE id=%s", (generation, run_id)
        )
        return {"generation": generation, "reset": sorted(affected)}
