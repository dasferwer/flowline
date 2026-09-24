import os
import secrets
import uuid
from contextlib import asynccontextmanager
from datetime import date, timedelta
from typing import Literal

from fastapi import Depends, FastAPI, Header, HTTPException
from psycopg.types.json import Jsonb
from pydantic import BaseModel, Field

from flowline.db import connect, init
from flowline.engine import create, validate


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
    delay: float = Field(default=0, ge=0, le=5)
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


@app.post("/runs/{identity}/cancel")
def cancel(identity: uuid.UUID):
    with connect() as conn:
        conn.execute("SELECT pg_advisory_xact_lock(340028)")
        changed = conn.execute(
            "UPDATE runs SET status='cancelled' WHERE id=%s AND status='running'", (identity,)
        )
        if changed.rowcount:
            conn.execute(
                "UPDATE nodes SET status='cancelled',token=NULL WHERE run_id=%s AND status IN ('pending','running')",
                (identity,),
            )
    return status(identity)
