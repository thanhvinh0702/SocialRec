# SocialRec
Hệ thống xử lý dữ liệu lớn theo kiến trúc Lambda cho ứng dụng đề xuất nội dung mạng xã hội

## Setup Environment
```
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

## Setup data
```
python3 generate_data.py
```

## Create Datasource
### Docker
```
cd data_source
docker compose up -d
```
### K8S
```
minikube mount "$(pwd)/hackaday_social_data_small:/mnt/socialrec-data"
```
```
eval "$(minikube docker-env)"
docker build -t socialrec-web:latest -f data_source/web/Dockerfile .
kubectl apply -k data_source/k8s
```

## Start Ingestion Layer
### Docker
```
cd ingestion
docker compose up -d
```
### K8S
```
cd ingestion/k8s

k apply -f secret.yaml -n socialrec
k apply -f minio-batch.yaml -n socialrec
k apply -f kafka-ui.yaml -n socialrec

# Setup Kafka cluster
sed -i 's/namespace: .*/namespace: socialrec/' strimzi-1.0.0/install/cluster-operator/*RoleBinding*.yaml
k apply -f ./strimzi-1.0.0/install/cluster-operator -n socialrec
k apply -f ./strimzi-1.0.0/kafka-cluster.yaml -n socialrec

# Setup Kafka connect cluster
eval "$(minikube docker-env)"
docker build -t my-kafka-connect:v1 ./connect
kubectl apply -f ./connect/kafka-connect.yaml -n socialrec
kubectl apply -f ./connect/kafka-connector-postgres.yaml -n socialrec
kubectl apply -f ./connect/kafka-connector-s3.yaml -n socialrec
```