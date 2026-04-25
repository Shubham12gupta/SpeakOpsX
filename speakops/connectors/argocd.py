import os
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
# ARGOCD CONNECTOR
# ═══════════════════════════════════════════════════════
class ArgocdConnector:

    def __init__(self, connector_config: Dict):
        self.config    = connector_config
        self.url       = self._resolve(connector_config.get("url", "")).rstrip("/")
        self.token     = self._resolve(connector_config.get("api_key", ""))
        self.apps      = connector_config.get("apps", {})
        self.connected = False
        self.session   = None

    def _resolve(self, value: str) -> str:
        if value and value.startswith("${") and value.endswith("}"):
            return os.environ.get(value[2:-1], "")
        return value or ""

    # ───────────────────────────────────────────────────
    # CONNECT
    # ───────────────────────────────────────────────────
    def connect(self) -> bool:
        if not self.url:
            raise ConnectionError("ArgoCD URL not configured")
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {self.token}",
            "Content-Type" : "application/json",
        })
        self.session.verify = False  # self-signed certs common in ArgoCD
        self.connected = True
        return True

    # ───────────────────────────────────────────────────
    # HEALTH CHECK
    # ───────────────────────────────────────────────────
    def health_check(self) -> bool:
        try:
            resp = self.session.get(f"{self.url}/api/v1/applications?limit=1", timeout=5)
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
            "sync_app"       : self.sync_app,
            "rollback_app"   : self.rollback_app,
            "get_app_health" : self.get_app_health,
            "get_out_of_sync": self.get_out_of_sync,
            "get_app_diff"   : self.get_app_diff,
            "get_all_apps"   : self.get_all_apps,
        }

        handler = routes.get(operation)
        if not handler:
            return err(operation, f"Unknown operation: {operation}")

        try:
            return handler(params)
        except requests.exceptions.ConnectionError:
            return err(operation, f"Cannot connect to ArgoCD at {self.url}")
        except requests.exceptions.Timeout:
            return err(operation, "ArgoCD request timed out")
        except Exception as e:
            return err(operation, str(e))

    # ───────────────────────────────────────────────────
    # SYNC APP
    # ───────────────────────────────────────────────────
    def sync_app(self, params: Dict) -> Dict:
        app = self._resolve_app(params.get("app", ""))
        if not app:
            return err("sync_app", "App name is required")

        resp = self.session.post(
            f"{self.url}/api/v1/applications/{app}/sync",
            json={"prune": False, "dryRun": False},
            timeout=30
        )

        if resp.status_code == 200:
            return ok("sync_app", {"app": app}, f"{app} sync triggered successfully")

        return err("sync_app", f"ArgoCD returned {resp.status_code}: {resp.text[:100]}")

    # ───────────────────────────────────────────────────
    # ROLLBACK APP
    # ───────────────────────────────────────────────────
    def rollback_app(self, params: Dict) -> Dict:
        app      = self._resolve_app(params.get("app", ""))
        revision = params.get("revision", 0)

        if not app:
            return err("rollback_app", "App name is required")

        resp = self.session.post(
            f"{self.url}/api/v1/applications/{app}/rollback",
            json={"id": int(revision)},
            timeout=30
        )

        if resp.status_code == 200:
            rev_text = f"revision {revision}" if revision else "previous revision"
            return ok("rollback_app", {"app": app, "revision": revision}, f"{app} rolled back to {rev_text}")

        return err("rollback_app", f"ArgoCD returned {resp.status_code}: {resp.text[:100]}")

    # ───────────────────────────────────────────────────
    # GET APP HEALTH
    # ───────────────────────────────────────────────────
    def get_app_health(self, params: Dict) -> Dict:
        app = self._resolve_app(params.get("app", ""))
        if not app:
            return err("get_app_health", "App name is required")

        resp = self.session.get(f"{self.url}/api/v1/applications/{app}", timeout=10)

        if resp.status_code == 404:
            return err("get_app_health", f"App '{app}' not found in ArgoCD")

        if resp.status_code != 200:
            return err("get_app_health", f"ArgoCD returned {resp.status_code}")

        data   = resp.json()
        health = data.get("status", {}).get("health", {})
        sync   = data.get("status", {}).get("sync", {})

        health_status = health.get("status", "Unknown")
        sync_status   = sync.get("status", "Unknown")

        summary = f"{app} — Health: {health_status} | Sync: {sync_status}"

        return ok("get_app_health", {
            "app"          : app,
            "health_status": health_status,
            "sync_status"  : sync_status,
            "message"      : health.get("message", ""),
        }, summary)

    # ───────────────────────────────────────────────────
    # GET OUT OF SYNC APPS
    # ───────────────────────────────────────────────────
    def get_out_of_sync(self, params: Dict) -> Dict:
        resp = self.session.get(
            f"{self.url}/api/v1/applications",
            timeout=10
        )

        if resp.status_code != 200:
            return err("get_out_of_sync", f"ArgoCD returned {resp.status_code}")

        apps        = resp.json().get("items", [])
        out_of_sync = []

        for app in apps:
            name        = app.get("metadata", {}).get("name", "")
            sync_status = app.get("status", {}).get("sync", {}).get("status", "")
            health      = app.get("status", {}).get("health", {}).get("status", "")

            if sync_status == "OutOfSync":
                out_of_sync.append({
                    "name"  : name,
                    "sync"  : sync_status,
                    "health": health,
                })

        if not out_of_sync:
            summary = "All apps are in sync"
        else:
            summary = f"{len(out_of_sync)} app(s) out of sync: {', '.join(a['name'] for a in out_of_sync)}"

        return ok("get_out_of_sync", {"out_of_sync": out_of_sync, "count": len(out_of_sync)}, summary)

    # ───────────────────────────────────────────────────
    # GET APP DIFF
    # ───────────────────────────────────────────────────
    def get_app_diff(self, params: Dict) -> Dict:
        app = self._resolve_app(params.get("app", ""))
        if not app:
            return err("get_app_diff", "App name is required")

        resp = self.session.get(
            f"{self.url}/api/v1/applications/{app}/resource-tree",
            timeout=10
        )

        if resp.status_code != 200:
            return err("get_app_diff", f"ArgoCD returned {resp.status_code}")

        data  = resp.json()
        nodes = data.get("nodes", [])

        diff_items = [
            n for n in nodes
            if n.get("health", {}).get("status") not in ["Healthy", None]
        ]

        summary = f"{app} — {len(diff_items)} resources with issues" if diff_items else f"{app} — all resources healthy"

        return ok("get_app_diff", {"app": app, "issues": diff_items}, summary)

    # ───────────────────────────────────────────────────
    # GET ALL APPS
    # ───────────────────────────────────────────────────
    def get_all_apps(self, params: Dict) -> Dict:
        resp = self.session.get(f"{self.url}/api/v1/applications", timeout=10)

        if resp.status_code != 200:
            return err("get_all_apps", f"ArgoCD returned {resp.status_code}")

        apps    = resp.json().get("items", [])
        summary_apps = []

        for app in apps:
            name   = app.get("metadata", {}).get("name", "")
            health = app.get("status", {}).get("health", {}).get("status", "Unknown")
            sync   = app.get("status", {}).get("sync", {}).get("status", "Unknown")
            summary_apps.append({"name": name, "health": health, "sync": sync})

        healthy    = sum(1 for a in summary_apps if a["health"] == "Healthy")
        out_sync   = sum(1 for a in summary_apps if a["sync"] == "OutOfSync")
        summary    = f"{len(summary_apps)} apps — {healthy} healthy, {out_sync} out of sync"

        return ok("get_all_apps", {"apps": summary_apps}, summary)

    # ───────────────────────────────────────────────────
    # RESOLVE APP NAME from config
    # ───────────────────────────────────────────────────
    def _resolve_app(self, app_name: str) -> str:
        if not app_name:
            return ""
        return self.apps.get(app_name, app_name)


# ═══════════════════════════════════════════════════════
# FACTORY
# ═══════════════════════════════════════════════════════
def create_connector(config_obj) -> ArgocdConnector:
    connector_cfg = {}
    if hasattr(config_obj, "connectors") and config_obj.connectors:
        argocd = config_obj.connectors.argocd
        if argocd:
            connector_cfg = argocd.model_dump() if hasattr(argocd, "model_dump") else {}

    connector = ArgocdConnector(connector_cfg)
    connector.connect()
    return connector


# ═══════════════════════════════════════════════════════
# CLI TEST
# ═══════════════════════════════════════════════════════
if __name__ == "__main__":
    print("\n🔌  Testing ArgoCD Connector\n")

    config = {
        "url"    : os.environ.get("ARGOCD_URL", "https://localhost:8080"),
        "api_key": os.environ.get("ARGOCD_TOKEN", ""),
        "apps"   : {
            "staging"   : "my-app-staging",
            "production": "my-app-production",
        }
    }

    connector = ArgocdConnector(config)

    try:
        connector.connect()
        print("✅  Connected\n")

        healthy = connector.health_check()
        print(f"🏥  Health: {'OK' if healthy else 'UNREACHABLE'}\n")

        if healthy:
            result = connector.execute("get_all_apps", {})
            print(f"📋  Apps: {result['summary']}")

            result = connector.execute("get_out_of_sync", {})
            print(f"🔄  Sync: {result['summary']}")

    except ConnectionError as e:
        print(f"❌  {e}")
        print("    Set ARGOCD_URL and ARGOCD_TOKEN env vars")