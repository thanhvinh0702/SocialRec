FROM python:3.10

WORKDIR /app

RUN pip install elasticsearch

COPY serving/scripts/create-index.py /app/create-index.py

CMD ["python", "/scripts/create-index.py"]