import logging
import os
import time

import pika

from flowline.db import init
from flowline.engine import claim, finish

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


def main():
    logging.basicConfig(level=logging.INFO)
    logging.getLogger("pika").setLevel(logging.WARNING)
    init()
    while True:
        try:
            node = claim()
            if node:
                try:
                    finish(node, execute(node))
                except Exception as exc:
                    logger.exception("Задача завершилась ошибкой")
                    finish(node, error=str(exc))
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
