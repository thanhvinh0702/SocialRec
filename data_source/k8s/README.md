# SocialRec on Kubernetes

These manifests keep the container images small:

- PostgreSQL reads CSV seed files from a Minikube-mounted host path
- MinIO uploads images from the same mounted host path
- the Flask app is the only custom image you build locally

## 1. Mount your local dataset into Minikube

Run this in a separate terminal and keep it open:

```bash
minikube mount "$(pwd)/hackaday_social_data_small:/mnt/socialrec-data"
```

If you want the full dataset instead:

```bash
minikube mount "$(pwd)/hackaday_social_data:/mnt/socialrec-data"
```

## 2. Build the web image inside Minikube

```bash
eval "$(minikube docker-env)"
docker build -t socialrec-web:latest -f data_source/web/Dockerfile .
```

## 3. Apply the manifests

```bash
kubectl apply -k data_source/k8s
```

## 4. Watch the pods

```bash
kubectl get pods -n socialrec -w
```

## 5. Open the app

```bash
minikube service socialrec-web -n socialrec
```

## Useful Commands

Check resources:

```bash
kubectl get all -n socialrec
```

Check logs:

```bash
kubectl logs deploy/postgres -n socialrec
kubectl logs deploy/socialrec-web -n socialrec
kubectl logs job/minio-bootstrap -n socialrec
```

If you change the image files and want to re-upload them:

```bash
kubectl delete job minio-bootstrap -n socialrec
kubectl apply -f data_source/k8s/minio.yaml
```

If you change `init.sql` or need a clean reseed:

```bash
kubectl delete namespace socialrec
kubectl apply -k data_source/k8s
```
