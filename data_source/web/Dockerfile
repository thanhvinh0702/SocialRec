FROM python:3.12-slim

WORKDIR /app

COPY data_source/web/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY data_source/web /app

ENV PYTHONUNBUFFERED=1

CMD ["python", "app.py"]
