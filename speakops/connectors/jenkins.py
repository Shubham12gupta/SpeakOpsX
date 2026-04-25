import requests
import pathlib
import yaml
from typing import Dict, Any, Optional
from requests.auth import HTTPBasicAuth


# ═══════════════════════════════════════════════════════
# RESULT BUILDER
# ═══════════════════════════════════════════════════════
def ok(operation: str, data: Any, summary: str) -> Dict:
    return {
        "success"  : True,
        "operation": operation,
        "data"     : data,
        "summary"  : summary,
        "error"    : None,
    }

def err(operation: str, error: str) -> Dict:
    return {
        "success"  : False,
        "operation": operation,
        "data"     : None,
        "summary"  : f"Failed: {error}",
        "error"    : error,
    }


# ═══════════════════════════════════════════════════════
# JENKINS CONNECTOR
# ═══════════════════════════════════════════════════════
class JenkinsConnector:

    def __init__(self, connector_config: Dict):
        self.config    = connector_config
        self.url       = self._resolve(connector_config.get("url", ""))
        self.token     = self._resolve(connector_config.get("api_key", ""))
        self.username  = self._resolve(connector_config.get("username", "admin"))
        self.jobs      = connector_config.get("jobs", {})
        self.connected = False
        self.session   = None

    # ───────────────────────────────────────────────────
    # RESOLVE ENV VARS
    # ───────────────────────────────────────────────────
    def _resolve(self, value: str) -> str:
        import os
        if value and value.startswith("${") and value.endswith("}"):
            var_name = value[2:-1]
            return os.environ.get(var_name, "")
        return value or ""

    # ───────────────────────────────────────────────────
    # CONNECT
    # ───────────────────────────────────────────────────
    def connect(self) -> bool:
        if not self.url:
            raise ConnectionError("Jenkins URL not configured")
        if not self.token:
            raise ConnectionError("Jenkins API key not configured")

        self.session = requests.Session()
        self.session.auth = HTTPBasicAuth(self.username, self.token)
        self.session.headers.update({
            "Content-Type": "application/json",
        })
        self.connected = True
        return True

    # ───────────────────────────────────────────────────
    # HEALTH CHECK
    # ───────────────────────────────────────────────────
    def health_check(self) -> bool:
        try:
            resp = self.session.get(
                f"{self.url}/api/json",
                timeout=5
            )
            return resp.status_code == 200
        except Exception:
            return False

    # ───────────────────────────────────────────────────
    # EXECUTE — MAIN ROUTER
    # ───────────────────────────────────────────────────
    def execute(self, operation: str, params: Dict) -> Dict:
        if not self.connected:
            self.connect()

        routes = {
            "trigger_build"       : self.trigger_build,
            "trigger_deploy"      : self.trigger_deploy,
            "get_pipeline_status" : self.get_pipeline_status,
            "cancel_pipeline"     : self.cancel_pipeline,
            "get_build_logs"      : self.get_build_logs,
            "get_all_jobs"        : self.get_all_jobs,
        }

        handler = routes.get(operation)
        if not handler:
            return err(operation, f"Unknown operation: {operation}")

        try:
            return handler(params)
        except requests.exceptions.ConnectionError:
            return err(operation, f"Cannot connect to Jenkins at {self.url}")
        except requests.exceptions.Timeout:
            return err(operation, "Jenkins request timed out")
        except Exception as e:
            return err(operation, str(e))

    # ───────────────────────────────────────────────────
    # TRIGGER BUILD
    # ───────────────────────────────────────────────────
    def trigger_build(self, params: Dict) -> Dict:
        branch   = params.get("branch", "main")
        job_name = params.get("job") or self.jobs.get("build", "")

        if not job_name:
            return err("trigger_build", "No build job configured")

        # get crumb for CSRF
        crumb = self._get_crumb()

        url  = f"{self.url}/job/{job_name}/buildWithParameters"
        data = {"BRANCH": branch}

        headers = {}
        if crumb:
            headers[crumb["crumbRequestField"]] = crumb["crumb"]

        resp = self.session.post(url, params=data, headers=headers, timeout=10)

        if resp.status_code in [200, 201]:
            return ok(
                "trigger_build",
                {"job": job_name, "branch": branch},
                f"Build triggered for {job_name} on branch {branch}"
            )

        return err("trigger_build", f"Jenkins returned {resp.status_code}: {resp.text[:100]}")

    # ───────────────────────────────────────────────────
    # TRIGGER DEPLOY
    # ───────────────────────────────────────────────────
    def trigger_deploy(self, params: Dict) -> Dict:
        branch      = params.get("branch", "main")
        environment = params.get("environment", "staging")
        job_name    = params.get("job") or self.jobs.get("deploy", "")

        if not job_name:
            return err("trigger_deploy", "No deploy job configured")

        crumb = self._get_crumb()

        url  = f"{self.url}/job/{job_name}/buildWithParameters"
        data = {
            "BRANCH"     : branch,
            "ENVIRONMENT": environment,
        }

        headers = {}
        if crumb:
            headers[crumb["crumbRequestField"]] = crumb["crumb"]

        resp = self.session.post(url, params=data, headers=headers, timeout=10)

        if resp.status_code in [200, 201]:
            return ok(
                "trigger_deploy",
                {"job": job_name, "branch": branch, "environment": environment},
                f"Deploy triggered — {branch} → {environment}"
            )

        return err("trigger_deploy", f"Jenkins returned {resp.status_code}")

    # ───────────────────────────────────────────────────
    # GET PIPELINE STATUS
    # ───────────────────────────────────────────────────
    def get_pipeline_status(self, params: Dict) -> Dict:
        job_name = params.get("job") or self.jobs.get("build", "")

        if not job_name:
            return err("get_pipeline_status", "No job name provided")

        url  = f"{self.url}/job/{job_name}/lastBuild/api/json"
        resp = self.session.get(url, timeout=10)

        if resp.status_code == 404:
            return err("get_pipeline_status", f"Job '{job_name}' not found")

        if resp.status_code != 200:
            return err("get_pipeline_status", f"Jenkins returned {resp.status_code}")

        data     = resp.json()
        result   = data.get("result", "IN_PROGRESS")
        building = data.get("building", False)
        number   = data.get("number", "?")
        duration = data.get("duration", 0)

        status = "RUNNING" if building else (result or "UNKNOWN")

        summary = f"Job {job_name} #{number} — {status}"
        if duration:
            summary += f" ({round(duration/1000)}s)"

        return ok(
            "get_pipeline_status",
            {
                "job"     : job_name,
                "build"   : number,
                "status"  : status,
                "building": building,
                "duration": duration,
            },
            summary
        )

    # ───────────────────────────────────────────────────
    # CANCEL PIPELINE
    # ───────────────────────────────────────────────────
    def cancel_pipeline(self, params: Dict) -> Dict:
        job_name = params.get("job") or self.jobs.get("build", "")

        if not job_name:
            return err("cancel_pipeline", "No job name provided")

        # get last build number
        url  = f"{self.url}/job/{job_name}/lastBuild/api/json"
        resp = self.session.get(url, timeout=10)

        if resp.status_code != 200:
            return err("cancel_pipeline", f"Could not get build info: {resp.status_code}")

        data     = resp.json()
        building = data.get("building", False)
        number   = data.get("number")

        if not building:
            return err("cancel_pipeline", f"Job {job_name} is not currently running")

        crumb = self._get_crumb()
        headers = {}
        if crumb:
            headers[crumb["crumbRequestField"]] = crumb["crumb"]

        stop_url = f"{self.url}/job/{job_name}/{number}/stop"
        resp     = self.session.post(stop_url, headers=headers, timeout=10)

        if resp.status_code in [200, 302]:
            return ok(
                "cancel_pipeline",
                {"job": job_name, "build": number},
                f"Build #{number} of {job_name} cancelled"
            )

        return err("cancel_pipeline", f"Jenkins returned {resp.status_code}")

    # ───────────────────────────────────────────────────
    # GET BUILD LOGS
    # ───────────────────────────────────────────────────
    def get_build_logs(self, params: Dict) -> Dict:
        job_name = params.get("job") or self.jobs.get("build", "")
        lines    = int(params.get("lines", 50))

        if not job_name:
            return err("get_build_logs", "No job name provided")

        url  = f"{self.url}/job/{job_name}/lastBuild/consoleText"
        resp = self.session.get(url, timeout=15)

        if resp.status_code != 200:
            return err("get_build_logs", f"Jenkins returned {resp.status_code}")

        log_lines = resp.text.strip().split("\n")
        last_lines = log_lines[-lines:]

        return ok(
            "get_build_logs",
            {"job": job_name, "lines": last_lines, "total": len(log_lines)},
            f"Last {len(last_lines)} lines from {job_name}"
        )

    # ───────────────────────────────────────────────────
    # GET ALL JOBS
    # ───────────────────────────────────────────────────
    def get_all_jobs(self, params: Dict) -> Dict:
        url  = f"{self.url}/api/json?tree=jobs[name,color,lastBuild[result,building]]"
        resp = self.session.get(url, timeout=10)

        if resp.status_code != 200:
            return err("get_all_jobs", f"Jenkins returned {resp.status_code}")

        jobs = resp.json().get("jobs", [])

        summary_jobs = []
        for j in jobs:
            last  = j.get("lastBuild") or {}
            color = j.get("color", "")
            status = "RUNNING" if last.get("building") else (last.get("result") or "NO BUILDS")
            summary_jobs.append({
                "name"  : j["name"],
                "status": status,
                "color" : color,
            })

        passing = sum(1 for j in summary_jobs if j["status"] == "SUCCESS")
        failing = sum(1 for j in summary_jobs if j["status"] == "FAILURE")
        running = sum(1 for j in summary_jobs if j["status"] == "RUNNING")

        summary = f"{len(summary_jobs)} jobs — {passing} passing, {failing} failing, {running} running"

        return ok("get_all_jobs", {"jobs": summary_jobs}, summary)

    # ───────────────────────────────────────────────────
    # CRUMB — CSRF protection
    # ───────────────────────────────────────────────────
    def _get_crumb(self) -> Optional[Dict]:
        try:
            resp = self.session.get(
                f"{self.url}/crumbIssuer/api/json",
                timeout=5
            )
            if resp.status_code == 200:
                return resp.json()
        except Exception:
            pass
        return None


# ═══════════════════════════════════════════════════════
# FACTORY
# ═══════════════════════════════════════════════════════
def create_connector(config_obj) -> JenkinsConnector:
    connector_cfg = {}
    if hasattr(config_obj, "connectors") and config_obj.connectors:
        jenkins = config_obj.connectors.jenkins
        if jenkins:
            connector_cfg = jenkins.model_dump() if hasattr(jenkins, "model_dump") else {}

    connector = JenkinsConnector(connector_cfg)
    connector.connect()
    return connector


# ═══════════════════════════════════════════════════════
# CLI TEST
# ═══════════════════════════════════════════════════════
if __name__ == "__main__":
    import os

    print("\n🔌  Testing Jenkins Connector\n")

    config = {
        "url"     : os.environ.get("JENKINS_URL", "http://localhost:8080"),
        "api_key" : os.environ.get("JENKINS_TOKEN", ""),
        "username": os.environ.get("JENKINS_USER", "admin"),
        "jobs"    : {
            "build" : "my-app-build",
            "deploy": "my-app-deploy",
        }
    }

    connector = JenkinsConnector(config)

    try:
        connector.connect()
        print("✅  Connected\n")

        healthy = connector.health_check()
        print(f"🏥  Health: {'OK' if healthy else 'UNREACHABLE'}\n")

        if healthy:
            result = connector.execute("get_all_jobs", {})
            print(f"📋  Jobs: {result['summary']}")

            result = connector.execute("get_pipeline_status", {"job": "my-app-build"})
            print(f"🔄  Status: {result['summary']}")

    except ConnectionError as e:
        print(f"❌  {e}")
        print("    Set JENKINS_URL and JENKINS_TOKEN env vars")