"""
RabbitMQ-backed queue client.

Provides `enqueue` and `dequeue` helpers used by the producer and consumer.
Requires the ``pika`` library and a running RabbitMQ instance.
"""

from typing import Optional

import pika

DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 5672
DEFAULT_QUEUE = "file_queue"
EOF_SENTINEL = b"__EOF__"


class QueueClient:
    """Blocking connection to a RabbitMQ queue."""

    def __init__(
        self,
        host: str = DEFAULT_HOST,
        port: int = DEFAULT_PORT,
        queue: str = DEFAULT_QUEUE,
    ):
        self.host = host
        self.port = port
        self.queue = queue
        self._connection: Optional[pika.BlockingConnection] = None
        self._channel: Optional[pika.adapters.blocking_connection.BlockingChannel] = (
            None
        )

    # -- connection management ------------------------------------------------

    def connect(self) -> None:
        self._connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=self.host, port=self.port),
        )
        self._channel = self._connection.channel()
        self._channel.queue_declare(queue=self.queue, durable=True)

    def close(self) -> None:
        if self._connection and not self._connection.is_closed:
            self._connection.close()
        self._connection = None
        self._channel = None

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, *exc):
        self.close()

    # -- public API -----------------------------------------------------------

    def enqueue(self, data: bytes) -> None:
        """Publish raw bytes to the queue."""
        if self._channel is None:
            raise RuntimeError("Not connected")
        self._channel.basic_publish(
            exchange="",
            routing_key=self.queue,
            body=data,
            properties=pika.BasicProperties(delivery_mode=2),
        )

    def dequeue(self) -> Optional[bytes]:
        """Fetch one message. Returns ``None`` when the queue is empty."""
        if self._channel is None:
            raise RuntimeError("Not connected")
        method, _properties, body = self._channel.basic_get(
            queue=self.queue,
            auto_ack=True,
        )
        if method is None:
            return None
        return body

    def queue_size(self) -> int:
        """Return the current number of messages in the queue."""
        if self._channel is None:
            raise RuntimeError("Not connected")
        res = self._channel.queue_declare(queue=self.queue, durable=True, passive=True)
        return res.method.message_count

    def health(self) -> bool:
        """Return ``True`` if the connection to RabbitMQ is alive."""
        try:
            if self._connection and self._connection.is_open:
                return True
            return False
        except Exception:
            return False
