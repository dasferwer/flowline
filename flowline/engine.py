import uuid
from graphlib import CycleError, TopologicalSorter

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
            lease_until=now()+interval '15 seconds' WHERE run_id=%s AND name=%s""",
            (token, node["run_id"], node["name"]),
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
            "SELECT * FROM nodes WHERE run_id=%s AND name=%s AND token=%s AND status='running' AND lease_until>now() FOR UPDATE",
            (node["run_id"], node["name"], node["token"]),
        ).fetchone()
        if run["status"] != "running" or not current:
            return False
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
