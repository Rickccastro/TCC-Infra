# app/main.py

from fastapi import FastAPI

from app.services.logs_service    import LogsService

app = FastAPI()


@app.get("/logs/http-status")
def http_status():
    try:
        return LogsService.get_http_status_distribution()
    except Exception as e:
        # Retorna o erro real em vez de 500 genérico
        return {"error": str(e), "type": type(e).__name__}


@app.get("/logs/avg-response-time")
def avg_response_time():
    return LogsService.get_avg_request_time()


@app.get("/logs/avg-upstream-time")
def avg_upstream_time():
    return LogsService.get_avg_upstream_time()


@app.get("/logs/p50-response-time")
def p50_response_time():
    return LogsService.get_p50_response_time()


@app.get("/logs/p95-response-time")
def p95_response_time():
    return LogsService.get_p95_response_time()


@app.get("/logs/p99-response-time")
def p99_response_time():
    return LogsService.get_p99_response_time()


@app.get("/logs/top-slowest-endpoints")
def top_slowest():
    return LogsService.get_top_slowest_endpoints()


@app.get("/logs/device-distribution")
def device_distribution():
    return LogsService.get_device_distribution()


@app.get("/logs/requests-per-second")
def requests_per_second():
    return LogsService.get_requests_per_second()


@app.get("/logs/client-abort-rate")
def client_abort_rate():

    return LogsService.get_client_abort_rate()


@app.get("/logs/login-avg-response")
def login_avg_response():
    return LogsService.get_login_avg_response_time()


@app.get("/logs/login-attempts")
def login_attempts():
    return LogsService.get_login_attempts()
