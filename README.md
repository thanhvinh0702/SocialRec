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

kubectl apply -k common

# Setup Kafka operator
sed -i 's/namespace: .*/namespace: socialrec/' strimzi-1.0.0/install/cluster-operator/*RoleBinding*.yaml
kubectl apply -f ./strimzi-1.0.0/install/cluster-operator -n socialrec
kubectl apply -f ./strimzi-1.0.0/kafka-cluster.yaml -n socialrec

# Setup Kafka connect cluster
eval "$(minikube docker-env)"
docker build -t my-kafka-connect:v1 ./connect
kubectl apply -f ./connect/kafka-connect.yaml -n socialrec
kubectl apply -f ./connect/kafka-connector-postgres.yaml -n socialrec
kubectl apply -f ./connect/kafka-connector-s3.yaml -n socialrec
```

## Start Batch Layer
### K8S
```
cd batch_layer

# Setup spark operator
helm repo add spark-operator https://kubeflow.github.io/spark-operator
helm install spark-operator spark-operator/spark-operator \
    --namespace socialrec \
    --set "spark.jobNamespaces={socialrec}"

# Setup Spark Application
eval "$(minikube docker-env)"
docker build -t socialrec-spark-preprocess:v1 ./spark/data_preprocess/silver
docker build -t socialrec-spark-feature-engineering:v1 ./spark/data_preprocess/gold
docker build -t socialrec-pytorch-embeddings:v1 ./pytorch/embeddings


# Setup Airflow
eval "$(minikube docker-env)"
docker build -t my-preprocess-dag:v1 ./airflow
kubectl apply -f ./airflow/airflow-sparkapplication-rbac.yaml

helm repo add apache-airflow https://airflow.apache.org
helm install airflow apache-airflow/airflow \
    --namespace socialrec \
    --set images.airflow.repository=my-preprocess-dag \
    --set images.airflow.tag=v1
    
# Light version
helm install airflow apache-airflow/airflow \
    --namespace socialrec \
    --set images.airflow.repository=my-preprocess-dag \
    --set images.airflow.tag=v1 \
    -f ./airflow/airflow-light.yaml
```