import os
import secrets
import uuid
from contextlib import asynccontextmanager
from datetime import date, datetime, timedelta
from typing import Literal

from fastapi import Depends, FastAPI, Header, HTTPException
from psycopg.types.json import Jsonb
from pydantic import BaseModel, Field

from flowline.db import connect, init
from flowline.engine import create, retry, validate


@asynccontextmanager
async def lifespan(app):
    init()
    yield


def authorize(x_api_key: str = Header(default="")):
    key = os.environ.get("API_KEY", "")
    if not key or not secrets.compare_digest(key, x_api_key):
        raise HTTPException(401, "Неверный API-ключ")


app = FastAPI(title="Flowline", lifespan=lifespan, dependencies=[Depends(authorize)])


class Node(BaseModel):
    name: str = Field(pattern=r"^[a-z][a-z0-9_]{0,49}$")
    task: Literal["constant", "sum", "divide"]
    value: int = Field(default=0, ge=-1000000, le=1000000)
    delay: float = Field(default=0, ge=0, le=120)
    dependencies: list[str] = Field(default_factory=list, max_length=50)


class Definition(BaseModel):
    name: str = Field(pattern=r"^[a-z][a-z0-9_-]{0,79}$")
    version: int = Field(ge=1)
    parallelism: int = Field(default=2, ge=1, le=4)
    daily: bool = False
    nodes: list[Node] = Field(min_length=1, max_length=50)


class Run(BaseModel):
    name: str
    version: int
    logical_date: date


class Backfill(Run):
    days: int = Field(ge=1, le=30)


@app.get("/health")
def health():
    with connect() as conn:
        conn.execute("SELECT 1")
    return {"status": "ok"}


@app.post("/definitions")
def definition(body: Definition):
    graph = {"nodes": [node.model_dump() for node in body.nodes], "parallelism": body.parallelism}
    try:
        validate(graph)
    except ValueError as exc:
        raise HTTPException(422, str(exc)) from exc
    with connect() as conn:
        added = conn.execute(
            """INSERT INTO definitions VALUES (%s,%s,%s,%s)
            ON CONFLICT DO NOTHING RETURNING name""",
            (body.name, body.version, Jsonb(graph), body.daily),
        ).fetchone()
        if not added:
            old = conn.execute(
                "SELECT graph,daily FROM definitions WHERE name=%s AND version=%s",
                (body.name, body.version),
            ).fetchone()
            if old["graph"] != graph or old["daily"] != body.daily:
                raise HTTPException(409, "Версия определения неизменяема")
    return {"name": body.name, "version": body.version}


def lookup(conn, body):
    row = conn.execute(
        "SELECT * FROM definitions WHERE name=%s AND version=%s", (body.name, body.version)
    ).fetchone()
    if not row:
        raise HTTPException(404, "Определение не найдено")
    return row


@app.post("/runs")
def start(body: Run):
    with connect() as conn:
        return {"id": create(conn, lookup(conn, body), body.logical_date, "current")}


@app.post("/backfills")
def backfill(body: Backfill):
    with connect() as conn:
        definition = lookup(conn, body)
        return {
            "ids": [
                create(conn, definition, body.logical_date + timedelta(days=i), "backfill")
                for i in range(body.days)
            ]
        }


@app.get("/runs/{identity}")
def status(identity: uuid.UUID):
    with connect() as conn:
        run = conn.execute("SELECT * FROM runs WHERE id=%s", (identity,)).fetchone()
        if not run:
            raise HTTPException(404, "Запуск не найден")
        return {
            **run,
            "nodes": conn.execute(
                "SELECT name,task,status,attempts,output,error FROM nodes WHERE run_id=%s ORDER BY name",
                (identity,),
            ).fetchall(),
        }


class NodeDiagnostic(BaseModel):
    name: str
    status: str
    attempts: int
    reasons: list[
        Literal[
            "ready",
            "dependencies",
            "retry_delay",
            "run_limit",
            "global_limit",
            "backfill_limit",
            "run_stopped",
            "running",
            "lease_expired",
            "attempt_deadline",
            "retry_exhausted",
            "failed",
            "completed",
            "cancelled",
        ]
    ]
    blocking_dependencies: list[str]
    lease_until: datetime | None
    available_at: datetime


class RunDiagnostics(BaseModel):
    id: uuid.UUID
    status: str
    observed_at: datetime
    nodes: list[NodeDiagnostic]


@app.get("/runs/{identity}/diagnostics", response_model=RunDiagnostics)
def diagnostics(identity: uuid.UUID):
    with connect() as conn:
        conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        observed = conn.execute("SELECT now() AS observed").fetchone()["observed"]
        run = conn.execute("SELECT * FROM runs WHERE id=%s", (identity,)).fetchone()
        if not run:
            raise HTTPException(404, "Запуск не найден")
        nodes = conn.execute(
            "SELECT * FROM nodes WHERE run_id=%s ORDER BY name", (identity,)
        ).fetchall()
        counts = conn.execute("""SELECT count(*) AS active,
            count(*) FILTER(WHERE r.kind='backfill') AS backfills
            FROM nodes n JOIN runs r ON r.id=n.run_id
            WHERE n.status='running' AND r.status='running'""").fetchone()
        by_name = {node["name"]: node for node in nodes}
        run_active = sum(node["status"] == "running" for node in nodes)
        result = []
        for node in nodes:
            reasons = []
            blockers = sorted(
                dep for dep in node["dependencies"] if by_name[dep]["status"] != "completed"
            )
            state = node["status"]
            if state == "pending":
                if run["status"] != "running":
                    reasons.append("run_stopped")
                else:
                    if blockers:
                        reasons.append("dependencies")
                    if node["available_at"] > observed:
                        reasons.append("retry_delay")
                    if run_active >= run["parallelism"]:
                        reasons.append("run_limit")
                    if counts["active"] >= 4:
                        reasons.append("global_limit")
                    if run["kind"] == "backfill" and counts["backfills"] >= 3:
                        reasons.append("backfill_limit")
                    if not reasons:
                        reasons.append("ready")
            elif state == "running":
                if run["status"] != "running":
                    reasons.append("run_stopped")
                elif node["lease_until"] is None or node["lease_until"] <= observed:
                    reasons.append("lease_expired")
                elif node["started_at"] is None or node["started_at"] <= observed - timedelta(
                    minutes=5
                ):
                    reasons.append("attempt_deadline")
                else:
                    reasons.append("running")
            elif state == "failed":
                reasons.append("retry_exhausted" if node["attempts"] >= 3 else "failed")
            else:
                reasons.append(state)
            result.append({**node, "reasons": reasons, "blocking_dependencies": blockers})
        return {"id": identity, "status": run["status"], "observed_at": observed, "nodes": result}


@app.post("/runs/{identity}/cancel")
def cancel(identity: uuid.UUID):
    with connect() as conn:
        conn.execute("SELECT pg_advisory_xact_lock(340028)")
        changed = conn.execute(
            "UPDATE runs SET status='cancelled' WHERE id=%s AND status='running'", (identity,)
        )
        if changed.rowcount:
            conn.execute(
                "UPDATE attempts SET status='cancelled',finished_at=clock_timestamp() WHERE run_id=%s AND status='running'",
                (identity,),
            )
            conn.execute(
                "UPDATE nodes SET status='cancelled',token=NULL WHERE run_id=%s AND status IN ('pending','running')",
                (identity,),
            )
    return status(identity)


class Retry(BaseModel):
    roots: list[str] = Field(min_length=1, max_length=50)


@app.post("/runs/{identity}/retry")
def repeat(identity: uuid.UUID, body: Retry):
    try:
        return retry(identity, body.roots)
    except ValueError as exc:
        raise HTTPException(409, str(exc)) from exc


@app.get("/runs/{identity}/history")
def history(identity: uuid.UUID):
    status(identity)
    with connect() as conn:
        return {
            "attempts": conn.execute(
                "SELECT * FROM attempts WHERE run_id=%s ORDER BY started_at,token", (identity,)
            ).fetchall(),
            "retries": conn.execute(
                "SELECT * FROM run_events WHERE run_id=%s ORDER BY id", (identity,)
            ).fetchall(),
        }
