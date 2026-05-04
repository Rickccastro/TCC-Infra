from app.collectors.loki_collector import LokiCollector


class LogsService:

    @staticmethod
    def get_http_status_distribution():

        query = """
                sum by (status_desc) (
                count_over_time(
                    {job="nginx"}
                    | regexp `"[A-Z]+ [^"]+" (?P<status>[0-9]{3}) `
                    | label_format status_desc="{{if eq .status \\"200\\"}}200 - OK{{else if eq .status \\"206\\"}}206 - Partial Content{{else if eq .status \\"301\\"}}301 - Redirect{{else if eq .status \\"302\\"}}302 - Found{{else if eq .status \\"303\\"}}303 - See Other{{else if eq .status \\"304\\"}}304 - Not Modified{{else if eq .status \\"400\\"}}400 - Bad Request{{else if eq .status \\"403\\"}}403 - Forbidden{{else if eq .status \\"404\\"}}404 - Not Found{{else if eq .status \\"405\\"}}405 - Method Not Allowed{{else if eq .status \\"407\\"}}407 - Proxy Authentication Required{{else if eq .status \\"408\\"}}408 - Request Timeout{{else if eq .status \\"499\\"}}499 - Client Closed Request{{else if eq .status \\"500\\"}}500 - Internal Server Error{{else if eq .status \\"502\\"}}502 - Bad Gateway{{else if eq .status \\"503\\"}}503 - Service Unavailable{{else}}{{.status}}{{end}}"
                    [1m]
                )
                )
                """

        raw = LokiCollector.query(query)

        return LogsService._normalize_status_distribution(raw)

    @staticmethod
    def _normalize_status_distribution(raw_data):

        result = raw_data.get("data", {}).get("result", [])

        summary = {"success": 0, "redirect": 0, "client_error": 0, "server_error": 0}

        for item in result:

            status = item.get("metric", {}).get("status_desc", "")
            values = item.get("values", [])

            count = sum(int(v[1]) for v in values)  # soma todos os pontos

            if status.startswith("2"):
                summary["success"] += count
            elif status.startswith("3"):
                summary["redirect"] += count
            elif status.startswith("4"):
                summary["client_error"] += count
            elif status.startswith("5"):
                summary["server_error"] += count

        return summary

    @staticmethod
    def get_avg_request_time():

        query = """
                avg(
                avg_over_time(
                    {job="nginx", type="access"} != "ELB-HealthChecker"
                    | regexp "(?P<request_time>[0-9.]+) [0-9.]+$"
                    | unwrap request_time
                    [5m]
                )
                )
                """

        raw = LokiCollector.query(query)

        return LogsService._normalize_avg_request_time(raw)

    @staticmethod
    def _normalize_avg_request_time(raw_data):

        result = raw_data.get("data", {}).get("result", [])

        if not result:
            return {"avg_response_time": 0, "status": "unknown"}

        values = result[0].get("values", [])

        if not values:
            return {"avg_response_time": 0, "status": "unknown"}

        avg_time = float(values[-1][1])

        return {
            "avg_response_time": avg_time,
            "status": (
                "critical"
                if avg_time > 1
                else "warning" if avg_time > 0.5 else "normal"
            ),
        }

    @staticmethod
    def get_avg_upstream_time():

        query = """
            avg(
            avg_over_time(
                {job="nginx", type="access"} != "ELB-HealthChecker"
                | regexp "[0-9.]+ (?P<upstream_response_time>[0-9.]+)$"
                | unwrap upstream_response_time
                [5m]
            )
            )
            """

        raw = LokiCollector.query(query)

        return LogsService._normalize_avg_upstream_time(raw)

    @staticmethod
    def _normalize_avg_upstream_time(raw_data):
        result = raw_data.get("data", {}).get("result", [])

        if not result:
            return {"avg_upstream_time": 0, "status": "unknown"}

        values = result[0].get("values", [])

        if not values:
            return {"avg_upstream_time": 0, "status": "unknown"}

        avg_time = float(values[-1][1])

        return {
            "avg_upstream_time": avg_time,
            "status": (
                "critical"
                if avg_time > 1
                else "warning" if avg_time > 0.5 else "normal"
            ),
        }

    @staticmethod
    def get_p50_response_time():
        query = """
            sum(
            quantile_over_time(
                0.50,
                {job="nginx", type="access"}
                | regexp "(?P<request_time>[0-9.]+)$"
                | unwrap request_time
                [5m]
            )
            )
            """

        raw = LokiCollector.query(query)

        return LogsService._normalize_p50_response_time(raw)

    @staticmethod
    def _normalize_p50_response_time(raw_data):

        result = raw_data.get("data", {}).get("result", [])

        if not result:
            return {"p50_response_time": 0, "status": "unknown"}

        values = result[0].get("values", [])

        if not values:
            return {"p50_response_time": 0, "status": "unknown"}

        p50 = float(values[-1][1])

        return {
            "p50_response_time": p50,
            "status": ("critical" if p50 > 1 else "warning" if p50 > 0.5 else "normal"),
        }

    @staticmethod
    def get_p95_response_time():
        query = """
            sum(
            quantile_over_time(
                0.95,
                {job="nginx", type="access"}
                | regexp "(?P<request_time>[0-9.]+)$"
                | unwrap request_time
                [5m]
            )
            )
            """

        raw = LokiCollector.query(query)

        return LogsService._normalize_p95_response_time(raw)

    @staticmethod
    def _normalize_p95_response_time(raw_data):

        result = raw_data.get("data", {}).get("result", [])

        if not result:
            return {"p95_response_time": 0, "status": "unknown"}

        values = result[0].get("values", [])

        if not values:
            return {"p95_response_time": 0, "status": "unknown"}

        p95 = float(values[-1][1])

        return {
            "p95_response_time": p95,
            "status": ("critical" if p95 > 2 else "warning" if p95 > 1 else "normal"),
        }

    @staticmethod
    def get_p99_response_time():

        query = """
            sum(
            quantile_over_time(
                0.99,
                {job="nginx", type="access"}
                | regexp "(?P<request_time>[0-9.]+)$"
                | unwrap request_time
                [5m]
            )
            )
            """

        raw = LokiCollector.query(query)

        return LogsService._normalize_p99_response_time(raw)

    @staticmethod
    def _normalize_p99_response_time(raw_data):

        result = raw_data.get("data", {}).get("result", [])

        if not result:
            return {"p99_response_time": 0, "status": "unknown"}

        values = result[0].get("values", [])

        if not values:
            return {"p99_response_time": 0, "status": "unknown"}

        p99 = float(values[-1][1])

        return {
            "p99_response_time": p99,
            "status": ("critical" if p99 > 5 else "warning" if p99 > 2 else "normal"),
        }

    @staticmethod
    def get_top_slowest_endpoints():

        query = """
             topk(
                10,
                quantile_over_time(
                    0.95,
                    {job="nginx", type="access"}
                    | json
                    | unwrap request_time
                    [5m]
                ) by (uri)
                )
            """

        raw = LokiCollector.query(query)

        return LogsService._normalize_top_slowest_endpoints(raw)

    @staticmethod
    def _normalize_top_slowest_endpoints(raw_data):

        result = raw_data.get("data", {}).get("result", [])

        endpoints = []

        for item in result:

            uri = item.get("metric", {}).get("uri", "unknown")

            values = item.get("values", [])
            value = float(values[-1][1]) if values else 0

            endpoints.append(
                {
                    "uri": uri,
                    "p95_response_time": value,
                    "status": (
                        "critical"
                        if value > 3
                        else "warning" if value > 1 else "normal"
                    ),
                }
            )

        # ordena do pior para o melhor
        endpoints.sort(key=lambda x: x["p95_response_time"], reverse=True)

        return endpoints

    @staticmethod
    def get_device_distribution():
        query = """
                sum by (device) (
                count_over_time(
                    {job="nginx"}
                    | json
                    | line_format `{{.http_user_agent}}`
                    !~ `(css|js|png|jpg|jpeg|gif|ico|svg|woff|woff2|ttf|map)`
                    !~ `(?i)bot|crawler|spider`
                    | regexp `(?P<device>Android|iPhone|iPad|Mobile|Tablet|Windows NT|Macintosh|Mac OS X|Linux x86_64|Ubuntu|X11|ChromeOS)`
                    | device != ""
                [1m])
                )
                """

        raw = LokiCollector.query(query)

        return LogsService._normalize_device_distribution(raw)

    @staticmethod
    def _normalize_device_distribution(raw_data):

        result = raw_data.get("data", {}).get("result", [])

        devices = {}

        total = 0

        for item in result:

            device = item.get("metric", {}).get("device", "unknown")

            values = item.get("values", [])
            count = int(values[-1][1]) if values else 0

            devices[device] = count
            total += count

        # calcular percentual (importante pro ML e análise)
        output = []

        for device, count in devices.items():

            percentage = (count / total * 100) if total > 0 else 0

            output.append(
                {"device": device, "count": count, "percentage": round(percentage, 2)}
            )

        # ordenar por mais usado
        output.sort(key=lambda x: x["count"], reverse=True)

        return output

    @staticmethod
    def get_requests_per_second():

        query = """
            sum by (metric) (
            rate(
                {job="nginx", type="access"}
                | label_format metric="nginx_http_requests_per_second"
            [1m])
            )
            """

        raw = LokiCollector.query(query)

        return LogsService._normalize_rps(raw)

    @staticmethod
    def _normalize_rps(raw_data):

        result = raw_data.get("data", {}).get("result", [])

        if not result:
            return {"requests_per_second": 0, "status": "unknown"}

        values = result[0].get("values", [])

        if not values:
            return {"requests_per_second": 0, "status": "unknown"}

        rps = float(values[-1][1])

        return {
            "requests_per_second": rps,
            "status": (
                "critical" if rps > 100 else "warning" if rps > 50 else "normal"
            ),
        }

    @staticmethod
    def get_client_abort_rate():

        query = """
            sum by (metric) (
            count_over_time(
                {job="nginx"} |= " 499 "
                | label_format metric="client_abort_499_per_minute"
            [1m])
            )
            """

        raw = LokiCollector.query(query)

        return LogsService._normalize_client_abort(raw)

    @staticmethod
    def _normalize_client_abort(raw_data):
        result = raw_data.get("data", {}).get("result", [])

        if not result:
            return {"client_abort_499_per_minute": 0, "status": "unknown"}

        values = result[0].get("values", [])
        count = int(values[-1][1]) if values else 0

        return {
            "client_abort_499_per_minute": count,
            "status": (
                "critical" if count > 50 else "warning" if count > 10 else "normal"
            ),
        }

    @staticmethod
    def get_login_avg_response_time():

        query = """
                avg_over_time(
                        {job="nginx"}
                        | json
                        | uri = "/login/index.php"
                        | method = "POST"
                        | unwrap request_time
                    [1m])
                  """

        raw = LokiCollector.query(query)

        return LogsService._normalize_login_avg(raw)

    @staticmethod
    def _normalize_login_avg(raw_data):
        result = raw_data.get("data", {}).get("result", [])

        if not result:
            return {"login_avg_response_seconds": 0, "status": "unknown"}

        values = result[0].get("values", [])
        value = float(values[-1][1]) if values else 0

        return {
            "login_avg_response_seconds": value,
            "status": (
                "critical" if value > 2 else "warning" if value > 1 else "normal"
            ),
        }

    @staticmethod
    def get_login_attempts():

        query = """
                sum by (endpoint) (
                count_over_time(
                    {job="nginx"}
                    | json
                    | method = "POST"
                    | uri = "/login/index.php"
                    | label_format endpoint="login"
                [1m])
                )
                """

        raw = LokiCollector.query(query)

        return LogsService._normalize_login_attempts(raw)

    @staticmethod
    def _normalize_login_attempts(raw_data):
        result = raw_data.get("data", {}).get("result", [])

        if not result:
            return {"login_attempts_per_minute": 0, "status": "unknown"}

        values = result[0].get("values", [])
        count = int(values[-1][1]) if values else 0

        return {
            "login_attempts_per_minute": count,
            "status": (
                "critical" if count > 300 else "warning" if count > 100 else "normal"
            ),
        }
