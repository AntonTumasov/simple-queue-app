# Simple Queue Service

A file-copy pipeline that uses **RabbitMQ** as the message broker. A producer reads lines from an input file, publishes them to a RabbitMQ queue over AMQP (TCP), and a consumer drains the queue into an output file — producing an exact copy. Both actions are initiated by a human sending a simple HTTP request to /produce endpoint.
In this particular case since we're writing to a file it overrides it every time instead of appending so sending the same request multiple times will result in the output file being identical to the input file. However, in the real world scenario the data would be stored in some long-term storage like S3 as Parquet files and rotated based on time or size. Alternatively, the files would have unique names based on timestamp or some kind of random identifier. Or if the data is a simple log line the consumer would send it to a logging service.
The RabbitMQ was chosen as a message broker for this implementation because it's an open-source, easy to use solution that can work in a cluster mode as well as in a single node mode. Also I have had some experience managing a number of production clusters in the past. Alternatively, if the solution is deployed in a cloud their native services could be used (SQS with SNS in AWS) or even Kafka (which could also be a managed AWS MSK cluster).

## Architecture

```
┌──────────┐   AMQP/TCP    ┌────────────┐   AMQP/TCP    ┌──────────┐
│ Producer │ ────────────▶ │  RabbitMQ  │ ◀──────────── │ Consumer │
│ (file →) │               │  (broker)  │               │ (→ file) │
└──────────┘               └────────────┘               └──────────┘
```

The solution uses EOF as a message delimiter to indicate the end of a file. Upon receiving the EOF message the consumer will close the output file and flush the buffer.
Also the consumer is part of the same deployment as a producer which in real production scenario would be a separate service scaled to handle the load.

## Quick Start (Docker Compose)

```bash
# Start RabbitMQ + app (consumer starts automatically in the background)
docker compose up --build -d

# Trigger the producer via HTTP — the consumer writes to /data/output.txt in real time
curl -X POST http://localhost:8080/produce -d '{"file": "/data/input.txt"}'

# Verify that the files are identical
diff data/input.txt data/output.txt
```

The `./data/` directory is bind-mounted into the container. The background consumer
writes incoming messages to the file configured by `OUTPUT_FILE` (default `/data/output.txt`).

## Running Tests

Unit tests mock pika and need **no running RabbitMQ**. The integration test is skipped automatically when RabbitMQ is unreachable.

```bash
pip install -r requirements.txt
python -m pytest test_queue.py -v
# or
python -m unittest test_queue -v
```

To include the integration end-to-end test, start RabbitMQ first:

```bash
docker run -d --name rabbitmq -p 5672:5672 rabbitmq:3-management-alpine
python -m pytest test_queue.py -v
```

## Kubernetes

Apply all manifests:

```bash
# We're using Kustomize here for the demo purposes. In production cluster we'd use Helm Chart with all the templates and variables.
kubectl apply -k k8s/
# Add the image to the local kind cluster so that it can run locally instead of pulling from a remote registry.
kind load docker-image simple-queue-service-app:latest
```

To test the service, first port-forward the queue-app service:

```bash
kubectl port-forward service/queue-app 8080:80
```

Then copy the input file to the pod (normally the file would be copied from a persistent volume or object storage):

```bash
kubectl cp data/input.txt <pod_name>:/data/input.txt
```

Then trigger the producer via HTTP:

```bash
curl -X POST http://localhost:8080/produce -d '{"file": "/data/input.txt"}'
```

Verify that the files are identical:

```bash
kubectl exec <pod_name> -- diff /data/input.txt /data/output.txt
```

### Resources created:

- **Deployment**
  - 2 replicas of `rabbitmq:3-management-alpine` with TCP liveness, readiness, and startup probes on port 5672.
  - 2 replicas of the `queue-app` with a simple `/health` endpoint for Kubernetes probes.
- **Service**
  - ClusterIP exposing AMQP (5672) and Management UI (15672)
  - ClusterIP exposing HTTP(80) for the queue-app.
- **PodDisruptionBudget** — at least 1 pod always available
- **HPA** — scales 2→10 replicas based on CPU (70%) and memory (80%)

## Project Structure

```
├── client.py            # Pika-based RabbitMQ client wrapper
├── producer.py          # File → RabbitMQ queue
├── consumer.py          # RabbitMQ queue → file
├── test_queue.py        # Unit tests (mocked) + integration test
├── requirements.txt     # pika
├── Dockerfile           # Producer / consumer image
├── docker-compose.yml   # RabbitMQ + app
└── k8s/
    ├── deployment.yaml  # Deployments
    └── hpa.yaml         # HorizontalPodAutoscalers
    └── namespace.yaml   # Namespace
    ├── pdb.yaml         # PodDisruptionBudgets
    └── secret.yaml      # RabbitMQ credentials
    ├── service.yaml     # ClusterIP Services
```

# Load testing
To use Kubernetes autoscaling the metrics-server needs to be installed in the cluster:

```bash
helm repo add metrics-server https://kubernetes-sigs.github.io/metrics-server/
helm install metrics-server metrics-server/metrics-server \
  --namespace kube-system \
  --set args='{--kubelet-insecure-tls,--kubelet-preferred-address-types=InternalIP}' \
  --set containerPort=4443
```

After the metrics-server is up and running, you can test the autoscaling by running the load test:

- Install `hey` as the simplest load testing tool:

```bash
brew install hey
```

- Then run the load test:

```bash
hey -n 1000 -c 10 -H "Content-Type: application/json" -m POST -D /data/input.txt http://localhost:8080/produce
```

**Important note**: when there's more than one consumer (>1 replica), the messages are distributed among the consumers in a round-robin fashion and because the processing of lines is sequential, the output file will contain only a portion of the input file, sometimes an EOF which is a separator but none of the pods get full file. Besides, those pods don't even have the input file copied to them hence the data is only partial.
For the sake of a simple demo, use a single replica. In real world this situation would be handled slightly differently with the partitioning and using a single consumer per partition. Or alternatively, some persistent storage like a DB or an S3 bucket.

# Observability and Monitoring + logging
There wasn't enough time to implement the solutions so as a future improvement there could be Opentelemetry installed for distributed tracing and metrics collection as well as some logging solution, e.g. FluentD or Loki.
