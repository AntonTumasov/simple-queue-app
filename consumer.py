"""
Consumer: dequeues lines from a RabbitMQ queue and writes them to an output file.

Two modes:
  - ``consume``          — poll-based, one-shot (used by tests and CLI).
  - ``consume_forever``  — push-based via ``basic_consume``, runs continuously
                           and writes each message to the output file in real
                           time as soon as it arrives.  The EOF sentinel is
                           treated as an end-of-batch marker (the file handle
                           is flushed) but the consumer keeps running.
"""

import argparse
import logging
import os
import time

import pika

from client import EOF_SENTINEL, QueueClient

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# One-shot polling consumer (tests / CLI)
# ---------------------------------------------------------------------------


def consume(
    filepath: str,
    host: str,
    port: int,
    queue: str = "file_queue",
    poll_interval: float = 0.2,
    max_empty: int = 50,
) -> int:
    """Dequeue lines and write them to *filepath*. Returns the number of lines written.

    The consumer keeps polling until it receives an EOF sentinel from the
    producer or has seen *max_empty* consecutive empty responses (safety net).
    """
    count = 0
    empty_streak = 0
    with QueueClient(host, port, queue) as client, open(filepath, "w") as fh:
        while True:
            item = client.dequeue()
            if item is None:
                empty_streak += 1
                if empty_streak >= max_empty:
                    logger.warning("Max empty polls reached — stopping.")
                    break
                time.sleep(poll_interval)
                continue
            if item == EOF_SENTINEL:
                logger.info("Received EOF sentinel — stopping.")
                break
            empty_streak = 0
            fh.write(item.decode())
            count += 1
    logger.info("Wrote %d lines to %s", count, filepath)
    return count


# ---------------------------------------------------------------------------
# Long-running push-based consumer (app server background thread)
# ---------------------------------------------------------------------------


def consume_forever(
    filepath: str,
    host: str,
    port: int,
    queue: str = "file_queue",
) -> None:
    """Subscribe to *queue* and write every message to *filepath* in real time.

    Blocks forever.  Each batch of messages (delimited by an EOF sentinel)
    overwrites the output file.  The file is opened in write mode when the
    first message of a batch arrives, and closed/flushed when the EOF sentinel
    is received — ready for the next batch.
    """
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=host, port=port),
    )
    channel = connection.channel()
    channel.queue_declare(queue=queue, durable=True)
    channel.basic_qos(prefetch_count=64)

    state = {"fh": None}
    logger.info("Consumer listening on queue '%s', writing to %s", queue, filepath)

    def _on_message(ch, method, _properties, body):
        if body == EOF_SENTINEL:
            if state["fh"] is not None:
                state["fh"].close()
                state["fh"] = None
            logger.info("Received EOF sentinel — batch complete.")
        else:
            if state["fh"] is None:
                state["fh"] = open(filepath, "w")
            state["fh"].write(body.decode())
        ch.basic_ack(delivery_tag=method.delivery_tag)

    channel.basic_consume(queue=queue, on_message_callback=_on_message)

    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        channel.stop_consuming()
    finally:
        if state["fh"] is not None:
            state["fh"].close()
        connection.close()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Consume from RabbitMQ and write to a file"
    )
    parser.add_argument("file", help="Path to the output file")
    parser.add_argument("--host", default=os.environ.get("RABBITMQ_HOST", "127.0.0.1"))
    parser.add_argument(
        "--port", type=int, default=int(os.environ.get("RABBITMQ_PORT", "5672"))
    )
    parser.add_argument("--queue", default=os.environ.get("QUEUE_NAME", "file_queue"))
    parser.add_argument(
        "--forever", action="store_true", help="Run in continuous push-based mode"
    )
    args = parser.parse_args()

    if args.forever:
        consume_forever(args.file, args.host, args.port, queue=args.queue)
    else:
        consume(args.file, args.host, args.port, queue=args.queue)


if __name__ == "__main__":
    main()
