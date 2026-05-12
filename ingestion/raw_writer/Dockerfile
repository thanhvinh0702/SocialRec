FROM python:3.12-slim

WORKDIR /app

COPY ingestion/raw_writer/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY ingestion/raw_writer /app

ENV PYTHONUNBUFFERED=1

CMD ["python", "writer.py"]
