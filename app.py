"""
HTTP API server with a background consumer.

The consumer subscribes to the RabbitMQ queue on startup and writes every
incoming message to OUTPUT_FILE in real time.  The only HTTP endpoint that
triggers work is ``POST /produce``.

Endpoints:
  POST /produce  {"file": "/data/input.txt"}  -> read file, publish lines to queue and write to output file.
  GET  /health                                -> liveness check
"""

import json
import logging
import os
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler

from consumer import consume_forever
from producer import produce

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

RABBITMQ_HOST = os.environ.get("RABBITMQ_HOST", "127.0.0.1")
RABBITMQ_PORT = int(os.environ.get("RABBITMQ_PORT", "5672"))
QUEUE_NAME = os.environ.get("QUEUE_NAME", "file_queue")
APP_PORT = int(os.environ.get("APP_PORT", "8080"))
OUTPUT_FILE = os.environ.get("OUTPUT_FILE", "/data/output.txt")


class Handler(BaseHTTPRequestHandler):

    def _send_json(self, status: int, body: dict) -> None:
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps(body).encode())

    def _read_body(self) -> dict:
        length = int(self.headers.get("Content-Length", 0))
        if length == 0:
            return {}
        return json.loads(self.rfile.read(length))

    # -- routes ---------------------------------------------------------------

    def do_GET(self):  # noqa: N802
        if self.path == "/health":
            self._send_json(200, {"status": "ok"})
        else:
            self._send_json(404, {"error": "not found"})

    def do_POST(self):  # noqa: N802
        if self.path == "/produce":
            self._handle_produce()
        else:
            self._send_json(404, {"error": "not found"})

    # -- handlers -------------------------------------------------------------

    def _handle_produce(self) -> None:
        body = self._read_body()
        filepath = body.get("file")
        if not filepath:
            self._send_json(400, {"error": "missing 'file' field"})
            return
        if not os.path.isfile(filepath):
            self._send_json(400, {"error": f"file not found: {filepath}"})
            return
        try:
            n = produce(filepath, RABBITMQ_HOST, RABBITMQ_PORT, QUEUE_NAME)
            self._send_json(200, {"status": "produced", "lines": n})
        except Exception as exc:
            logger.exception("Produce failed")
            self._send_json(500, {"error": str(exc)})

    def log_message(self, format, *args):
        logger.info("%s %s", self.client_address[0], format % args)


class ThreadedHTTPServer(HTTPServer):
    """Handle each request in a new thread so produce doesn't block health checks."""

    def process_request(self, request, client_address):
        t = threading.Thread(target=self._handle, args=(request, client_address))
        t.daemon = True
        t.start()

    def _handle(self, request, client_address):
        try:
            self.finish_request(request, client_address)
        except Exception:
            self.handle_error(request, client_address)
        finally:
            self.shutdown_request(request)


def _start_background_consumer() -> threading.Thread:
    """Start the push-based consumer in a daemon thread."""
    t = threading.Thread(
        target=consume_forever,
        args=(OUTPUT_FILE, RABBITMQ_HOST, RABBITMQ_PORT, QUEUE_NAME),
        daemon=True,
    )
    t.start()
    logger.info("Background consumer started, writing to %s", OUTPUT_FILE)
    return t


def main() -> None:
    _start_background_consumer()

    server = ThreadedHTTPServer(("0.0.0.0", APP_PORT), Handler)
    logger.info("App server listening on 0.0.0.0:%s", APP_PORT)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        logger.info("Shutting down.")
        server.shutdown()


if __name__ == "__main__":
    main()
