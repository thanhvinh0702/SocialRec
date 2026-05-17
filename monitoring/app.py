from fastapi import FastAPI, Request
from fastapi.responses import Response

from prometheus_client import Counter, Histogram, generate_latest
import time

app = FastAPI()

REQUEST_COUNT = Counter(
    "http_requests_total",
    "Total HTTP requests",
    ["method", "endpoint", "status"]
)

REQUEST_LATENCY = Histogram(
    "http_request_duration_seconds",
    "Request latency",
    ["endpoint"]
)

@app.middleware("http")
async def monitor_all_requests(request: Request, call_next):

    start_time = time.time()

    try:
        response = await call_next(request)
        status_code = response.status_code

    except Exception as e:
        status_code = 500
        raise e

    process_time = time.time() - start_time

    endpoint = request.url.path
    method = request.method

    # metrics update
    REQUEST_COUNT.labels(
        method=method,
        endpoint=endpoint,
        status=status_code
    ).inc()

    REQUEST_LATENCY.labels(endpoint=endpoint).observe(process_time)

    return response

@app.get("/metrics")
def metrics():
    return Response(generate_latest(), media_type="text/plain")