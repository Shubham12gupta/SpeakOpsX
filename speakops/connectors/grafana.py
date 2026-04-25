import os
import webbrowser
import requests
from typing import Dict, Any, Optional


# ═══════════════════════════════════════════════════════
# RESULT BUILDER
# ═══════════════════════════════════════════════════════
def ok(operation: str, data: Any, summary: str) -> Dict:
    return {"success": True, "operation": operation, "data": data, "summary": summary, "error": None}

def err(operation: str, error: str) -> Dict:
    return {"success": False, "operation": operation, "data": None, "summary": f"Failed: {error}", "error": error}


# ═══════════════════════════════════════════════════════
# GRAFANA CONNECTOR
# ═══════════════════════════════════════════════════════
class GrafanaConnector:

    def __init__(self, connector_config: Dict):
        self.config     = connector_config
        self.url        = self._resolve(connector_config.get("url", "")).rstrip("/")
        self.token      = self._resolve(connector_config.get("api_key", ""))
        self.dashboards = connector_config.get("dashboards", {})
        self.connected  = False
        self.session    = None

    def _resolve(self, value: str) -> str:
        if value and value.startswith("${") and value.endswith("}"):
            return os.environ.get(value[2:-1], "")
        return value or ""

    # ───────────────────────────────────────────────────
    # CONNECT
    # ───────────────────────────────────────────────────
    def connect(self) -> bool:
        if not self.url:
            raise ConnectionError("Grafana URL not configured")

        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {self.token}",
            "Content-Type" : "application/json",
        })
        self.connected = True
        return True

    # ───────────────────────────────────────────────────
    # HEALTH CHECK
    # ───────────────────────────────────────────────────
    def health_check(self) -> bool:
        try:
            resp = self.session.get(f"{self.url}/api/health", timeout=5)
            return resp.status_code == 200
        except Exception:
            return False

    # ───────────────────────────────────────────────────
    # EXECUTE ROUTER
    # ───────────────────────────────────────────────────
    def execute(self, operation: str, params: Dict) -> Dict:
        if not self.connected:
            self.connect()

        routes = {
            "open_dashboard" : self.open_dashboard,
            "fetch_metrics"  : self.fetch_metrics,
            "get_alerts"     : self.get_alerts,
            "get_dashboards" : self.get_dashboards,
            "get_datasources": self.get_datasources,
        }

        handler = routes.get(operation)
        if not handler:
            return err(operation, f"Unknown operation: {operation}")

        try:
            return handler(params)
        except requests.exceptions.ConnectionError:
            return err(operation, f"Cannot connect to Grafana at {self.url}")
        except requests.exceptions.Timeout:
            return err(operation, "Grafana request timed out")
        except Exception as e:
            return err(operation, str(e))

    # ───────────────────────────────────────────────────
    # OPEN DASHBOARD
    # ───────────────────────────────────────────────────
    def open_dashboard(self, params: Dict) -> Dict:
        dashboard_key = params.get("dashboard", "default")
        dashboard_uid = self.dashboards.get(dashboard_key, dashboard_key)

        if not dashboard_uid:
            dashboard_url = f"{self.url}/dashboards"
        else:
            resp = self.session.get(
                f"{self.url}/api/dashboards/uid/{dashboard_uid}",
                timeout=10
            )
            if resp.status_code == 200:
                slug = resp.json().get("meta", {}).get("slug", dashboard_uid)
                dashboard_url = f"{self.url}/d/{dashboard_uid}/{slug}"
            else:
                dashboard_url = f"{self.url}/dashboards"

        try:
            webbrowser.open(dashboard_url)
            opened = True
        except Exception:
            opened = False

        return ok(
            "open_dashboard",
            {"dashboard": dashboard_key, "url": dashboard_url, "opened": opened},
            f"Dashboard '{dashboard_key}' opened — {dashboard_url}"
        )

    # ───────────────────────────────────────────────────
    # FETCH METRICS
    # ───────────────────────────────────────────────────
    def fetch_metrics(self, params: Dict) -> Dict:
        service = params.get("service", "")

        # get datasources first
        ds_resp = self.session.get(f"{self.url}/api/datasources", timeout=10)
        if ds_resp.status_code != 200:
            return err("fetch_metrics", "Could not fetch datasources")

        datasources = ds_resp.json()
        prometheus  = next(
            (d for d in datasources if d.get("type") == "prometheus"),
            None
        )

        if not prometheus:
            return ok(
                "fetch_metrics",
                {"service": service, "note": "No Prometheus datasource found"},
                f"No Prometheus datasource configured in Grafana"
            )

        ds_uid = prometheus.get("uid", "")

        # query basic metrics
        queries = []
        if service:
            queries = [
                f'rate(http_requests_total{{service="{service}"}}[5m])',
                f'histogram_quantile(0.99, rate(http_request_duration_seconds_bucket{{service="{service}"}}[5m]))',
            ]
        else:
            queries = [
                'sum(rate(http_requests_total[5m])) by (service)',
            ]

        metrics_data = []
        for query in queries:
            resp = self.session.post(
                f"{self.url}/api/ds/query",
                json={
                    "queries": [{
                        "datasourceId": prometheus.get("id"),
                        "expr"        : query,
                        "refId"       : "A",
                    }],
                    "from": "now-5m",
                    "to"  : "now",
                },
                timeout=10
            )
            if resp.status_code == 200:
                results = resp.json().get("results", {})
                metrics_data.append({
                    "query": query,
                    "data" : results,
                })

        summary = f"Metrics fetched for {'service: ' + service if service else 'all services'}"
        return ok("fetch_metrics", {"metrics": metrics_data, "service": service}, summary)

    # ───────────────────────────────────────────────────
    # GET ALERTS
    # ───────────────────────────────────────────────────
    def get_alerts(self, params: Dict) -> Dict:
        resp = self.session.get(
            f"{self.url}/api/alertmanager/grafana/api/v2/alerts",
            timeout=10
        )

        if resp.status_code == 404:
            # try legacy endpoint
            resp = self.session.get(f"{self.url}/api/alerts", timeout=10)

        if resp.status_code != 200:
            return err("get_alerts", f"Grafana returned {resp.status_code}")

        alerts = resp.json()
        if not isinstance(alerts, list):
            alerts = []

        firing   = [a for a in alerts if a.get("state") == "alerting" or a.get("status", {}).get("state") == "active"]
        pending  = [a for a in alerts if a.get("state") == "pending"]
        ok_alerts= [a for a in alerts if a.get("state") == "ok"]

        if not firing and not pending:
            summary = "No active alerts — all clear"
        else:
            parts = []
            if firing : parts.append(f"{len(firing)} firing")
            if pending: parts.append(f"{len(pending)} pending")
            summary = "Alerts: " + ", ".join(parts)

        return ok(
            "get_alerts",
            {
                "firing" : firing[:5],
                "pending": pending[:5],
                "ok"     : len(ok_alerts),
                "total"  : len(alerts),
            },
            summary
        )

    # ───────────────────────────────────────────────────
    # GET DASHBOARDS LIST
    # ───────────────────────────────────────────────────
    def get_dashboards(self, params: Dict) -> Dict:
        resp = self.session.get(
            f"{self.url}/api/search?type=dash-db",
            timeout=10
        )

        if resp.status_code != 200:
            return err("get_dashboards", f"Grafana returned {resp.status_code}")

        dashboards = resp.json()
        dash_list  = [
            {"title": d.get("title"), "uid": d.get("uid"), "url": d.get("url")}
            for d in dashboards
        ]

        return ok(
            "get_dashboards",
            {"dashboards": dash_list, "count": len(dash_list)},
            f"{len(dash_list)} dashboards available"
        )

    # ───────────────────────────────────────────────────
    # GET DATASOURCES
    # ───────────────────────────────────────────────────
    def get_datasources(self, params: Dict) -> Dict:
        resp = self.session.get(f"{self.url}/api/datasources", timeout=10)

        if resp.status_code != 200:
            return err("get_datasources", f"Grafana returned {resp.status_code}")

        sources = resp.json()
        ds_list = [
            {"name": d.get("name"), "type": d.get("type"), "uid": d.get("uid")}
            for d in sources
        ]

        return ok(
            "get_datasources",
            {"datasources": ds_list},
            f"{len(ds_list)} datasources: {', '.join(d['name'] for d in ds_list)}"
        )


# ═══════════════════════════════════════════════════════
# FACTORY
# ═══════════════════════════════════════════════════════
def create_connector(config_obj) -> GrafanaConnector:
    connector_cfg = {}
    if hasattr(config_obj, "connectors") and config_obj.connectors:
        grafana = config_obj.connectors.grafana
        if grafana:
            connector_cfg = grafana.model_dump() if hasattr(grafana, "model_dump") else {}

    connector = GrafanaConnector(connector_cfg)
    connector.connect()
    return connector


# ═══════════════════════════════════════════════════════
# CLI TEST
# ═══════════════════════════════════════════════════════
if __name__ == "__main__":
    print("\n🔌  Testing Grafana Connector\n")

    config = {
        "url"       : os.environ.get("GRAFANA_URL", "http://localhost:3000"),
        "api_key"   : os.environ.get("GRAFANA_TOKEN", ""),
        "dashboards": {
            "default": "cluster-overview",
            "infra"  : "infrastructure-health",
        }
    }

    connector = GrafanaConnector(config)

    try:
        connector.connect()
        print("✅  Connected\n")

        healthy = connector.health_check()
        print(f"🏥  Health: {'OK' if healthy else 'UNREACHABLE'}\n")

        if healthy:
            result = connector.execute("get_dashboards", {})
            print(f"📊  Dashboards: {result['summary']}")

            result = connector.execute("get_alerts", {})
            print(f"🔔  Alerts: {result['summary']}")

    except ConnectionError as e:
        print(f"❌  {e}")
        print("    Set GRAFANA_URL and GRAFANA_TOKEN env vars")