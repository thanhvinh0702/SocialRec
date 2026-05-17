# Speed Layer

Speed Layer computes and serves low-latency recommendation outputs.

## Step 1. Deploy Redis

Redis is the serving/output store for Speed Layer, so deploy it before Flink.

These manifests deploy Redis into the existing `socialrec` namespace:

```bash
kubectl apply -k speed_layer/k8s
```

Check Redis:

```bash
kubectl get pods,svc,pvc -n socialrec -l app.kubernetes.io/component=redis
kubectl logs deploy/redis -n socialrec
```

Internal Redis endpoint for Flink:

```text
redis.socialrec.svc.cluster.local:6379
```

Quick connectivity test:

```bash
kubectl run redis-cli -n socialrec --rm -it --restart=Never --image=redis:7.2-alpine -- \
  redis-cli -h redis ping
```

## Step 2. Install Flink Kubernetes Operator

Install the Apache Flink Kubernetes Operator with Helm. The operator should be available before creating any `FlinkDeployment` resources.

This setup:

- installs operator release `1.14.0`
- installs the operator into namespace `socialrec`
- watches only namespace `socialrec`
- disables webhooks so cert-manager is not required for local development

```bash
helm repo add flink-kubernetes-operator-1.14.0 https://archive.apache.org/dist/flink/flink-kubernetes-operator-1.14.0/
helm repo update flink-kubernetes-operator-1.14.0
helm upgrade --install flink-kubernetes-operator \
  flink-kubernetes-operator-1.14.0/flink-kubernetes-operator \
  --namespace socialrec \
  --create-namespace \
  --values speed_layer/flink-operator/values.yaml
```

Check the operator:

```bash
kubectl get pods -n socialrec -l app.kubernetes.io/name=flink-kubernetes-operator
kubectl get crd | grep flink.apache.org
kubectl logs deploy/flink-kubernetes-operator -n socialrec
```

Uninstall:

```bash
helm uninstall flink-kubernetes-operator -n socialrec
```

## Step 3. Create the Flink Job ServiceAccount

The Flink job pods use the `flink-job` ServiceAccount. It is included in the Speed Layer Kubernetes manifests:

```bash
kubectl apply -k speed_layer/k8s
```

Check RBAC:

```bash
kubectl get serviceaccount,role,rolebinding -n socialrec -l app.kubernetes.io/component=flink-job
kubectl auth can-i create pods --as=system:serviceaccount:socialrec:flink-job -n socialrec
kubectl auth can-i create configmaps --as=system:serviceaccount:socialrec:flink-job -n socialrec
```

Use this ServiceAccount in `FlinkDeployment` specs:

```yaml
spec:
  serviceAccount: flink-job
```

## Step 4. Run the Kafka to Redis Flink Job

This job verifies that Flink can connect to Kafka and read Debezium events. It logs raw events and writes interaction create events (`op = c`) to Redis.

The Kubernetes Debezium connector currently uses `topic.prefix: postgres`, so the deployed job reads:

```text
postgres.public.interactions
```

Build the image inside Minikube:

```bash
eval "$(minikube docker-env)"
docker build -t socialrec-flink-kafka-log:latest -f speed_layer/flink_jobs/kafka_log/Dockerfile speed_layer/flink_jobs/kafka_log
```

Deploy:

```bash
kubectl apply -k speed_layer/k8s
```

Check the `FlinkDeployment`:

```bash
kubectl get flinkdeployment socialrec-kafka-log -n socialrec
kubectl get pods -n socialrec | grep socialrec-kafka-log
```

Confirm the Kafka topic has Debezium data:

```bash
kubectl exec -n socialrec kafka-cluster-dual-role-0 -c kafka -- \
  bin/kafka-console-consumer.sh \
  --bootstrap-server kafka-cluster-kafka-bootstrap:9092 \
  --topic postgres.public.interactions \
  --from-beginning \
  --max-messages 1 \
  --timeout-ms 5000
```

Check Flink TaskManager logs:

```bash
kubectl logs -n socialrec -l app=socialrec-kafka-log,component=taskmanager --tail=100
```

Expected log line shape:

```text
Debezium interaction event: {"before":null,"after":{...},"source":{...},"op":"r",...}
```

Check Redis output:

```bash
kubectl exec -it -n socialrec redis-master-0 -- redis-cli
ZREVRANGE trending:global 0 10 WITHSCORES
LRANGE user:1:recent_views 0 10
```

For the local Redis manifest in this repository, the pod is a Deployment named `redis`:

```bash
kubectl exec -it -n socialrec deploy/redis -- redis-cli
```
