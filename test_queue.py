"""
Unit and integration tests for the queue service.

Tests cover:
  - QueueClient enqueue / dequeue (mocked pika — no broker needed)
  - producer.produce function (mocked client)
  - consumer.consume function (mocked client)
  - Integration: end-to-end file round-trip (requires a running RabbitMQ)
"""

import collections
import os
import socket
import tempfile
import textwrap
import unittest
from unittest import mock


from client import EOF_SENTINEL, QueueClient
from producer import produce
from consumer import consume, consume_forever

RABBITMQ_HOST = os.environ.get("RABBITMQ_HOST", "127.0.0.1")
RABBITMQ_PORT = int(os.environ.get("RABBITMQ_PORT", "5672"))
INTEGRATION_QUEUE = "test_file_queue"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _rabbitmq_available() -> bool:
    """Quick TCP check to see if RabbitMQ is reachable."""
    try:
        s = socket.create_connection((RABBITMQ_HOST, RABBITMQ_PORT), timeout=2)
        s.close()
        return True
    except OSError:
        return False


def _make_mock_channel(queue: collections.deque):
    """Return a mock channel backed by a simple deque so enqueue/dequeue work."""
    channel = mock.MagicMock()

    def _publish(exchange, routing_key, body, properties=None):
        queue.append(body)

    def _basic_get(queue=None, auto_ack=True):  # noqa: ARG001
        # reuse outer `queue` via closure — the param name is ignored
        return _basic_get._deque_get()

    def _deque_get():
        if queue:
            body = queue.popleft()
            method = mock.MagicMock()
            return method, mock.MagicMock(), body
        return None, None, None

    _basic_get._deque_get = _deque_get

    channel.basic_publish.side_effect = _publish
    channel.basic_get.side_effect = _basic_get
    channel.queue_declare.return_value = mock.MagicMock(
        method=mock.MagicMock(message_count=0),
    )
    return channel


# ---------------------------------------------------------------------------
# 1. QueueClient unit tests (pika mocked out)
# ---------------------------------------------------------------------------
class TestQueueClientUnit(unittest.TestCase):

    def setUp(self):
        self._queue: collections.deque = collections.deque()
        self._mock_channel = _make_mock_channel(self._queue)
        self._patcher = mock.patch("client.pika.BlockingConnection")
        mock_conn_cls = self._patcher.start()
        self._mock_conn = mock.MagicMock()
        self._mock_conn.is_closed = False
        self._mock_conn.is_open = True
        self._mock_conn.channel.return_value = self._mock_channel
        mock_conn_cls.return_value = self._mock_conn

    def tearDown(self):
        self._patcher.stop()

    def test_enqueue_publishes(self):
        with QueueClient() as c:
            c.enqueue(b"hello")
        self.assertEqual(self._queue.popleft(), b"hello")

    def test_dequeue_returns_message(self):
        self._queue.append(b"world")
        with QueueClient() as c:
            result = c.dequeue()
        self.assertEqual(result, b"world")

    def test_dequeue_empty_returns_none(self):
        with QueueClient() as c:
            result = c.dequeue()
        self.assertIsNone(result)

    def test_enqueue_dequeue_fifo_order(self):
        with QueueClient() as c:
            c.enqueue(b"first")
            c.enqueue(b"second")
            self.assertEqual(c.dequeue(), b"first")
            self.assertEqual(c.dequeue(), b"second")

    def test_health_when_connected(self):
        with QueueClient() as c:
            self.assertTrue(c.health())

    def test_health_when_disconnected(self):
        c = QueueClient()
        self.assertFalse(c.health())

    def test_enqueue_not_connected_raises(self):
        c = QueueClient()
        with self.assertRaises(RuntimeError):
            c.enqueue(b"x")

    def test_dequeue_not_connected_raises(self):
        c = QueueClient()
        with self.assertRaises(RuntimeError):
            c.dequeue()


# ---------------------------------------------------------------------------
# 2. Producer unit tests (QueueClient mocked)
# ---------------------------------------------------------------------------
class TestProduceUnit(unittest.TestCase):

    def setUp(self):
        self._queue: collections.deque = collections.deque()
        self._mock_channel = _make_mock_channel(self._queue)
        self._patcher = mock.patch("client.pika.BlockingConnection")
        mock_conn_cls = self._patcher.start()
        mock_conn = mock.MagicMock()
        mock_conn.is_closed = False
        mock_conn.is_open = True
        mock_conn.channel.return_value = self._mock_channel
        mock_conn_cls.return_value = mock_conn

    def tearDown(self):
        self._patcher.stop()

    def test_produce_enqueues_all_lines(self):
        content = "line 1\nline 2\nline 3\n"
        with tempfile.NamedTemporaryFile("w", suffix=".txt", delete=False) as f:
            f.write(content)
            path = f.name
        try:
            n = produce(path, "127.0.0.1", 5672)
            self.assertEqual(n, 3)
            self.assertEqual(len(self._queue), 4)  # 3 lines + EOF sentinel
            self.assertEqual(self._queue[0], b"line 1\n")
            self.assertEqual(self._queue[1], b"line 2\n")
            self.assertEqual(self._queue[2], b"line 3\n")
            self.assertEqual(self._queue[3], EOF_SENTINEL)
        finally:
            os.unlink(path)

    def test_produce_empty_file(self):
        with tempfile.NamedTemporaryFile("w", suffix=".txt", delete=False) as f:
            path = f.name
        try:
            n = produce(path, "127.0.0.1", 5672)
            self.assertEqual(n, 0)
            self.assertEqual(len(self._queue), 1)  # just the EOF sentinel
            self.assertEqual(self._queue[0], EOF_SENTINEL)
        finally:
            os.unlink(path)


# ---------------------------------------------------------------------------
# 3. Consumer unit tests (QueueClient mocked)
# ---------------------------------------------------------------------------
class TestConsumeUnit(unittest.TestCase):

    def setUp(self):
        self._queue: collections.deque = collections.deque()
        self._mock_channel = _make_mock_channel(self._queue)
        self._patcher = mock.patch("client.pika.BlockingConnection")
        mock_conn_cls = self._patcher.start()
        mock_conn = mock.MagicMock()
        mock_conn.is_closed = False
        mock_conn.is_open = True
        mock_conn.channel.return_value = self._mock_channel
        mock_conn_cls.return_value = mock_conn

    def tearDown(self):
        self._patcher.stop()

    def test_consume_writes_all_lines(self):
        self._queue.extend([b"alpha\n", b"beta\n", b"gamma\n", EOF_SENTINEL])
        with tempfile.NamedTemporaryFile("w", suffix=".txt", delete=False) as f:
            path = f.name
        try:
            n = consume(path, "127.0.0.1", 5672)
            self.assertEqual(n, 3)
            with open(path) as fh:
                self.assertEqual(fh.read(), "alpha\nbeta\ngamma\n")
        finally:
            os.unlink(path)

    def test_consume_stops_on_eof_sentinel(self):
        self._queue.append(EOF_SENTINEL)
        with tempfile.NamedTemporaryFile("w", suffix=".txt", delete=False) as f:
            path = f.name
        try:
            n = consume(path, "127.0.0.1", 5672)
            self.assertEqual(n, 0)
        finally:
            os.unlink(path)

    def test_consume_stops_on_empty(self):
        with tempfile.NamedTemporaryFile("w", suffix=".txt", delete=False) as f:
            path = f.name
        try:
            n = consume(path, "127.0.0.1", 5672, max_empty=3, poll_interval=0.01)
            self.assertEqual(n, 0)
        finally:
            os.unlink(path)


# ---------------------------------------------------------------------------
# 4. consume_forever unit test (mocked pika)
# ---------------------------------------------------------------------------
class TestConsumeForeverUnit(unittest.TestCase):

    def _run_with_messages(self, messages, path):
        """Helper: deliver *messages* via a mocked basic_consume, return ack count."""
        delivered = []
        mock_channel = mock.MagicMock()
        mock_conn = mock.MagicMock()
        mock_conn.channel.return_value = mock_channel

        def _capture_consume(queue, on_message_callback):
            delivered.append(on_message_callback)

        mock_channel.basic_consume.side_effect = _capture_consume

        def _simulate_delivery():
            cb = delivered[0]
            for body in messages:
                method = mock.MagicMock()
                method.delivery_tag = id(body)
                cb(mock_channel, method, mock.MagicMock(), body)
            raise KeyboardInterrupt

        mock_channel.start_consuming.side_effect = _simulate_delivery

        with mock.patch("consumer.pika.BlockingConnection", return_value=mock_conn):
            consume_forever(path, "127.0.0.1", 5672)

        return mock_channel.basic_ack.call_count

    def test_single_batch(self):
        """A single batch of lines terminated by EOF produces the correct file."""
        messages = [b"line A\n", b"line B\n", EOF_SENTINEL]
        with tempfile.NamedTemporaryFile("w", suffix=".txt", delete=False) as f:
            path = f.name
        try:
            acks = self._run_with_messages(messages, path)
            with open(path) as fh:
                self.assertEqual(fh.read(), "line A\nline B\n")
            self.assertEqual(acks, 3)
        finally:
            os.unlink(path)

    def test_second_batch_overwrites(self):
        """A second batch after EOF overwrites the output file."""
        messages = [b"old\n", EOF_SENTINEL, b"new\n", EOF_SENTINEL]
        with tempfile.NamedTemporaryFile("w", suffix=".txt", delete=False) as f:
            path = f.name
        try:
            acks = self._run_with_messages(messages, path)
            with open(path) as fh:
                self.assertEqual(fh.read(), "new\n")
            self.assertEqual(acks, 4)
        finally:
            os.unlink(path)


# ---------------------------------------------------------------------------
# 5. End-to-end round-trip (requires running RabbitMQ)
# ---------------------------------------------------------------------------
@unittest.skipUnless(
    _rabbitmq_available(), "RabbitMQ not reachable — skipping integration tests"
)
class TestEndToEnd(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # Purge the integration queue before running
        try:
            with QueueClient(RABBITMQ_HOST, RABBITMQ_PORT, INTEGRATION_QUEUE) as c:
                c._channel.queue_purge(queue=INTEGRATION_QUEUE)
        except Exception:
            pass

    def test_file_round_trip(self):
        content = textwrap.dedent(
            """\
            The quick brown fox jumps over the lazy dog.
            Line 2: special chars ~!@#$%^&*()_+-=[]{}|;':\",./<>?
            Line 3: numbers 0123456789
            Line 4: tabs\there\tand\tthere
            Last line without trailing newline.
        """
        )

        with tempfile.NamedTemporaryFile("w", suffix=".txt", delete=False) as src:
            src.write(content)
            src_path = src.name

        dst_path = src_path + ".out"
        try:
            produce(src_path, RABBITMQ_HOST, RABBITMQ_PORT, INTEGRATION_QUEUE)
            consume(
                dst_path,
                RABBITMQ_HOST,
                RABBITMQ_PORT,
                queue=INTEGRATION_QUEUE,
            )

            with open(dst_path, "r") as fh:
                result = fh.read()
            self.assertEqual(result, content)
        finally:
            os.unlink(src_path)
            if os.path.exists(dst_path):
                os.unlink(dst_path)


if __name__ == "__main__":
    unittest.main()
