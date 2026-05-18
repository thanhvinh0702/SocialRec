#!/bin/bash

set -e

eval $(minikube docker-env)

echo "Build recommender image..."
docker build -t recommender:latest ./recommend-api

echo "Create Cassandra init ConfigMap..."

# xoá cái cũ nếu có (tránh stale)
kubectl delete configmap cassandra-init-sql --ignore-not-found
kubectl delete configmap cassandra-seed-sql --ignore-not-found

kubectl create configmap cassandra-init-sql \
  --from-file=init.cql=./recommender/init.cql

kubectl create configmap cassandra-seed-sql \
  --from-file=seed.cql=./recommender/seed.cql
  
echo "Deploy Cassandra..."
kubectl apply -f k8s/cassandra/

echo "Deploy Elasticsearch..."
kubectl apply -f k8s/elastic-search/

echo "Deploy Recommender..."
kubectl apply -f k8s/recommender/

echo "Done"