"""
Producer: reads lines from a file and publishes each line to a RabbitMQ queue.
"""

import argparse
import logging
import os
import sys

from client import EOF_SENTINEL, QueueClient

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)


def produce(filepath: str, host: str, port: int, queue: str = "file_queue") -> int:
    """Read *filepath* and enqueue every line. Returns the number of lines sent."""
    with open(filepath, "r") as fh:
        lines = fh.readlines()

    with QueueClient(host, port, queue) as client:
        for line in lines:
            client.enqueue(line.encode())
        client.enqueue(EOF_SENTINEL)
        logger.info("Enqueued %d lines from %s", len(lines), filepath)
    return len(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description="Publish file lines to RabbitMQ")
    parser.add_argument("file", help="Path to the input file")
    parser.add_argument("--host", default=os.environ.get("RABBITMQ_HOST", "127.0.0.1"))
    parser.add_argument(
        "--port", type=int, default=int(os.environ.get("RABBITMQ_PORT", "5672"))
    )
    parser.add_argument("--queue", default=os.environ.get("QUEUE_NAME", "file_queue"))
    args = parser.parse_args()

    if not os.path.isfile(args.file):
        logger.error("File not found: %s", args.file)
        sys.exit(1)

    produce(args.file, args.host, args.port, args.queue)


if __name__ == "__main__":
    main()
