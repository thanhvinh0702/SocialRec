kubectl apply -f k8s/namespace.yaml
kubectl apply -f k8s/node-exporter

kubectl apply -f k8s/kube-state-metrics

kubectl apply -f k8s/grafana

kubectl apply -f k8s/prometheus

nohup kubectl port-forward svc/grafana -n monitoring 3000:3000 > grafana.log 2>&1 &
nohup kubectl port-forward svc/prometheus -n monitoring 9090:9090 > prometheus.log 2>&1 &