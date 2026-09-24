import logging
import os
import threading
import time

import pika
from psycopg import Error

from flowline.db import init
from flowline.engine import claim, finish, renew

logger = logging.getLogger(__name__)


def execute(node):
    time.sleep(node["delay"])
    if node["task"] == "constant":
        value = node["value"]
    elif node["task"] == "sum":
        value = sum(node["inputs"]) + node["value"]
    else:
        value = sum(node["inputs"]) // node["value"]
    if not -(2**63) <= value < 2**63:
        raise ValueError("Результат не помещается в bigint")
    return value


def process(node, heartbeat_interval=3):
    stopped = threading.Event()

    def heartbeat():
        while not stopped.wait(heartbeat_interval):
            try:
                if not renew(node):
                    return
            except Error:
                return

    thread = threading.Thread(target=heartbeat, daemon=True)
    thread.start()
    try:
        try:
            return finish(node, execute(node))
        except Exception as exc:
            logger.exception("Задача завершилась ошибкой")
            return finish(node, error=str(exc))
    finally:
        stopped.set()
        thread.join(timeout=2)


def main():
    logging.basicConfig(level=logging.INFO)
    logging.getLogger("pika").setLevel(logging.WARNING)
    init()
    while True:
        try:
            node = claim()
            if node:
                process(node)
                continue
            try:
                with pika.BlockingConnection(pika.URLParameters(os.environ["AMQP_URL"])) as broker:
                    channel = broker.channel()
                    channel.queue_declare(queue="ready", durable=True)
                    method, _, _ = channel.basic_get(queue="ready", auto_ack=False)
                    if method:
                        channel.basic_ack(method.delivery_tag)
            except (pika.exceptions.AMQPError, OSError):
                pass
        except Exception:
            logger.exception("Ошибка цикла воркера")
        time.sleep(0.25)


if __name__ == "__main__":
    main()
