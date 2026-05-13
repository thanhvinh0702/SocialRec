# Ingestion Layer on Kubernetes

These manifests are designed to work with the existing datasource manifests in `data_source/k8s`.

They assume:

- the `socialrec` namespace is used
- the datasource PostgreSQL service is available as `postgres`
- the datasource k8s Postgres manifest has logical replication enabled

## Components

- Kafka broker
- Kafka UI
- Debezium Kafka Connect
- connector init job for PostgreSQL CDC
- dedicated batch MinIO instance
- MinIO bucket bootstrap job
- raw-writer deployment that stores CDC events in MinIO

## Before You Apply

Apply the datasource first:

```bash
kubectl apply -k data_source/k8s
```

Build the raw writer image inside Minikube:

```bash
eval "$(minikube docker-env)"
docker build -t socialrec-raw-writer:latest -f ingestion/raw_writer/Dockerfile .
```

## Apply Ingestion

```bash
kubectl apply -k ingestion/k8s
```

## Access

Kafka UI:

```bash
minikube service kafka-ui -n socialrec
```

Kafka Connect:

```bash
kubectl port-forward svc/connect 8083:8083 -n socialrec
```

Batch MinIO:

```bash
kubectl port-forward svc/minio-batch 9100:9000 9101:9001 -n socialrec
```

## Notes

- the first connector start performs Debezium `snapshot.mode=initial`
- after that, CDC continues streaming INSERT/UPDATE/DELETE changes
- `raw-writer` writes append-only JSONL files into the dedicated batch MinIO bucket `socialrec-batch-raw`

## Re-run Bootstrap Jobs

If you need to re-register the connector:

```bash
kubectl delete job connector-init -n socialrec
kubectl apply -f ingestion/k8s/connect.yaml
```

If you need to recreate the batch MinIO bucket:

```bash
kubectl delete job batch-minio-bootstrap -n socialrec
kubectl apply -f ingestion/k8s/minio-batch.yaml
```
