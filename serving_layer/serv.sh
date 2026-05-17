#!/bin/bash

eval $(minikube docker-env)

docker build -t recommender:latest ./api

kubectl apply -f k8s/cassandra/
kubectl apply -f k8s/elasticsearch/
kubectl apply -f k8s/recommender/