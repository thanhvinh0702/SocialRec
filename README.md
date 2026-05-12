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
```
minikube mount "$(pwd)/hackaday_social_data_small:/mnt/socialrec-data"
```
```
eval "$(minikube docker-env)"
docker build -t socialrec-web:latest -f data_source/web/Dockerfile .
kubectl apply -k data_source/k8s
minikube service socialrec-web -n socialrec
```