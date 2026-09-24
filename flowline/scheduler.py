import logging
import os
import time
from datetime import UTC, datetime

import pika

from flowline.db import connect, init
from flowline.engine import create

logger = logging.getLogger(__name__)


def tick(day=None):
    day = day or datetime.now(UTC).date()
    with connect() as conn:
        definitions = conn.execute(
            "SELECT DISTINCT ON (name) * FROM definitions ORDER BY name,version DESC"
        ).fetchall()
        return [
            create(conn, definition, day, "current")
            for definition in definitions
            if definition["daily"]
        ]


def main():
    logging.basicConfig(level=logging.INFO)
    logging.getLogger("pika").setLevel(logging.WARNING)
    init()
    previous = set()
    while True:
        try:
            current = set(tick())
            if current - previous:
                try:
                    with pika.BlockingConnection(
                        pika.URLParameters(os.environ["AMQP_URL"])
                    ) as broker:
                        channel = broker.channel()
                        channel.queue_declare(queue="ready", durable=True)
                        channel.basic_publish(
                            exchange="",
                            routing_key="ready",
                            body="ready",
                            properties=pika.BasicProperties(delivery_mode=2),
                        )
                except (pika.exceptions.AMQPError, OSError):
                    logger.warning("Воркеры найдут задания опросом PostgreSQL")
            previous = current
        except Exception:
            logger.exception("Не удалось создать плановые запуски")
        time.sleep(2)


if __name__ == "__main__":
    main()
