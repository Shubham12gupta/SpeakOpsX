import subprocess
import datetime
from typing import Dict, Any, Optional, List
from kubernetes import client, config
from kubernetes.client.rest import ApiException


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
# KUBERNETES CONNECTOR
# ═══════════════════════════════════════════════════════
class KubernetesConnector:

    def __init__(self, connector_config: Dict, namespace: str = "default"):
        self.connector_config = connector_config
        self.namespace        = namespace
        self.apps_v1          = None
        self.core_v1          = None
        self.connected        = False

    # ───────────────────────────────────────────────────
    # CONNECT
    # ───────────────────────────────────────────────────
    def connect(self) -> bool:
        try:
            config.load_kube_config()
            self.apps_v1   = client.AppsV1Api()
            self.core_v1   = client.CoreV1Api()
            self.connected = True
            return True
        except config.ConfigException:
            try:
                config.load_incluster_config()
                self.apps_v1   = client.AppsV1Api()
                self.core_v1   = client.CoreV1Api()
                self.connected = True
                return True
            except Exception as e:
                raise ConnectionError(f"Cannot connect to Kubernetes cluster: {e}")

    # ───────────────────────────────────────────────────
    # HEALTH CHECK
    # ───────────────────────────────────────────────────
    def health_check(self) -> bool:
        try:
            v1 = client.CoreV1Api()
            v1.list_namespace(limit=1)
            return True
        except Exception:
            return False

    # ───────────────────────────────────────────────────
    # EXECUTE — MAIN ROUTER
    # ───────────────────────────────────────────────────
    def execute(self, operation: str, params: Dict) -> Dict:
        if not self.connected:
            self.connect()

        routes = {
            "scale"              : self.scale,
            "rollout_restart"    : self.restart,
            "rollout_undo"       : self.rollback,
            "get_pods"           : self.get_pods,
            "fetch_logs"         : self.fetch_logs,
            "get_resource_usage" : self.get_resource_usage,
            "get_failing_pods"   : self.get_failing_pods,
            "get_events"         : self.get_events,
            "helm_upgrade"       : self.helm_upgrade,
            "helm_rollback"      : self.helm_rollback,
            "auto_diagnose"      : self.auto_diagnose,
        }

        handler = routes.get(operation)
        if not handler:
            return err(operation, f"Unknown operation: {operation}")

        try:
            return handler(params)
        except ApiException as e:
            return err(operation, f"K8s API error {e.status}: {e.reason}")
        except Exception as e:
            return err(operation, str(e))

    # ───────────────────────────────────────────────────
    # SCALE DEPLOYMENT
    # ───────────────────────────────────────────────────
    def scale(self, params: Dict) -> Dict:
        service  = params.get("service")
        replicas = int(params.get("replicas", 1))
        ns       = params.get("namespace", self.namespace)

        if not service:
            return err("scale", "service param is required")

        # check deployment exists
        try:
            self.apps_v1.read_namespaced_deployment(name=service, namespace=ns)
        except ApiException as e:
            if e.status == 404:
                return err("scale", f"Deployment '{service}' not found in namespace '{ns}'")
            raise

        # patch replicas
        body = {"spec": {"replicas": replicas}}
        self.apps_v1.patch_namespaced_deployment_scale(
            name=service, namespace=ns, body=body
        )

        return ok(
            "scale",
            {"service": service, "replicas": replicas, "namespace": ns},
            f"{service} scaled to {replicas} replicas in {ns}"
        )

    # ───────────────────────────────────────────────────
    # ROLLING RESTART
    # ───────────────────────────────────────────────────
    def restart(self, params: Dict) -> Dict:
        service = params.get("service")
        ns      = params.get("namespace", self.namespace)

        if not service:
            return err("rollout_restart", "service param is required")

        now = datetime.datetime.utcnow().isoformat() + "Z"
        body = {
            "spec": {
                "template": {
                    "metadata": {
                        "annotations": {
                            "kubectl.kubernetes.io/restartedAt": now
                        }
                    }
                }
            }
        }

        try:
            self.apps_v1.patch_namespaced_deployment(
                name=service, namespace=ns, body=body
            )
        except ApiException as e:
            if e.status == 404:
                return err("rollout_restart", f"Deployment '{service}' not found")
            raise

        return ok(
            "rollout_restart",
            {"service": service, "namespace": ns, "restarted_at": now},
            f"{service} rolling restart triggered successfully"
        )

    # ───────────────────────────────────────────────────
    # ROLLBACK
    # ───────────────────────────────────────────────────
    def rollback(self, params: Dict) -> Dict:
        service = params.get("service")
        ns      = params.get("namespace", self.namespace)

        if not service:
            return err("rollout_undo", "service param is required")

        result = subprocess.run(
            ["kubectl", "rollout", "undo", f"deployment/{service}", "-n", ns],
            capture_output=True, text=True
        )

        if result.returncode != 0:
            return err("rollout_undo", result.stderr.strip())

        return ok(
            "rollout_undo",
            {"service": service, "namespace": ns},
            f"{service} rolled back to previous version"
        )

    # ───────────────────────────────────────────────────
    # GET PODS
    # ───────────────────────────────────────────────────
    def get_pods(self, params: Dict) -> Dict:
        ns = params.get("namespace", self.namespace)

        pods_list = self.core_v1.list_namespaced_pod(namespace=ns)

        pods = []
        for pod in pods_list.items:
            restarts = 0
            if pod.status.container_statuses:
                restarts = sum(
                    cs.restart_count for cs in pod.status.container_statuses
                )

            age = ""
            if pod.metadata.creation_timestamp:
                delta = datetime.datetime.now(datetime.timezone.utc) - pod.metadata.creation_timestamp
                hours = int(delta.total_seconds() // 3600)
                age   = f"{hours}h" if hours > 0 else f"{int(delta.total_seconds() // 60)}m"

            pods.append({
                "name"    : pod.metadata.name,
                "status"  : pod.status.phase,
                "restarts": restarts,
                "age"     : age,
                "node"    : pod.spec.node_name,
            })

        running = sum(1 for p in pods if p["status"] == "Running")
        pending = sum(1 for p in pods if p["status"] == "Pending")
        failed  = sum(1 for p in pods if p["status"] == "Failed")

        summary = f"{running} running"
        if pending: summary += f", {pending} pending"
        if failed:  summary += f", {failed} failed"

        return ok("get_pods", {"pods": pods, "total": len(pods)}, summary)

    # ───────────────────────────────────────────────────
    # FETCH LOGS
    # ───────────────────────────────────────────────────
    def fetch_logs(self, params: Dict) -> Dict:
        service = params.get("service") or params.get("target")
        ns      = params.get("namespace", self.namespace)
        since   = params.get("since", "10m")

        if not service:
            return err("fetch_logs", "service param is required")

        # find pod for this service
        pods = self.core_v1.list_namespaced_pod(
            namespace=ns,
            label_selector=f"app={service}"
        )

        if not pods.items:
            # try without label selector — match by name prefix
            all_pods = self.core_v1.list_namespaced_pod(namespace=ns)
            matching = [
                p for p in all_pods.items
                if p.metadata.name.startswith(service)
                and p.status.phase == "Running"
            ]
            if not matching:
                return err("fetch_logs", f"No running pod found for service '{service}'")
            pod_name = matching[0].metadata.name
        else:
            running = [p for p in pods.items if p.status.phase == "Running"]
            if not running:
                return err("fetch_logs", f"No running pod for '{service}'")
            pod_name = running[0].metadata.name

        # parse since duration
        since_seconds = _parse_duration_to_seconds(since)

        logs = self.core_v1.read_namespaced_pod_log(
            name=pod_name,
            namespace=ns,
            tail_lines=100,
            since_seconds=since_seconds,
        )

        lines = logs.strip().split("\n") if logs.strip() else []

        return ok(
            "fetch_logs",
            {"pod": pod_name, "service": service, "lines": lines, "count": len(lines)},
            f"Last {since} logs for {service} — {len(lines)} lines from pod {pod_name}"
        )

    # ───────────────────────────────────────────────────
    # RESOURCE USAGE
    # ───────────────────────────────────────────────────
    def get_resource_usage(self, params: Dict) -> Dict:
        ns = params.get("namespace", self.namespace)

        result = subprocess.run(
            ["kubectl", "top", "pods", "-n", ns, "--no-headers"],
            capture_output=True, text=True
        )

        if result.returncode != 0:
            return err("get_resource_usage", "metrics-server may not be installed: " + result.stderr.strip())

        usage = []
        for line in result.stdout.strip().split("\n"):
            if not line.strip():
                continue
            parts = line.split()
            if len(parts) >= 3:
                usage.append({
                    "pod"   : parts[0],
                    "cpu"   : parts[1],
                    "memory": parts[2],
                })

        summary = f"{len(usage)} pods — top usage: {usage[0]['pod']} {usage[0]['cpu']} CPU {usage[0]['memory']} MEM" if usage else "No usage data"

        return ok("get_resource_usage", {"usage": usage}, summary)

    # ───────────────────────────────────────────────────
    # GET FAILING PODS — SRE Incident Mode
    # ───────────────────────────────────────────────────
    def get_failing_pods(self, params: Dict) -> Dict:
        ns = params.get("namespace", self.namespace)

        pods_list = self.core_v1.list_namespaced_pod(namespace=ns)

        failing    = []
        crash_loop = []
        pending    = []
        oom_killed = []

        for pod in pods_list.items:
            name   = pod.metadata.name
            phase  = pod.status.phase
            cs_list = pod.status.container_statuses or []

            if phase == "Failed":
                failing.append(name)

            if phase == "Pending":
                pending.append(name)

            for cs in cs_list:
                if cs.restart_count > 3:
                    crash_loop.append({"pod": name, "restarts": cs.restart_count})

                if cs.last_state and cs.last_state.terminated:
                    reason = cs.last_state.terminated.reason or ""
                    if reason == "OOMKilled":
                        oom_killed.append(name)

        issues = len(failing) + len(crash_loop) + len(pending) + len(oom_killed)

        if issues == 0:
            summary = "All pods healthy — no issues detected"
        else:
            parts = []
            if failing:    parts.append(f"{len(failing)} failing")
            if crash_loop: parts.append(f"{len(crash_loop)} crash looping")
            if pending:    parts.append(f"{len(pending)} pending")
            if oom_killed: parts.append(f"{len(oom_killed)} OOMKilled")
            summary = " | ".join(parts)

        return ok(
            "get_failing_pods",
            {
                "failing"   : failing,
                "crash_loop": crash_loop,
                "pending"   : pending,
                "oom_killed": oom_killed,
                "total_issues": issues,
            },
            summary
        )

    # ───────────────────────────────────────────────────
    # GET EVENTS
    # ───────────────────────────────────────────────────
    def get_events(self, params: Dict) -> Dict:
        ns    = params.get("namespace", self.namespace)
        since = params.get("since", "1h")

        events_list = self.core_v1.list_namespaced_event(namespace=ns)

        since_seconds = _parse_duration_to_seconds(since)
        cutoff = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(seconds=since_seconds)

        events = []
        for event in events_list.items:
            if event.type != "Warning":
                continue

            event_time = event.last_timestamp or event.event_time
            if event_time and event_time < cutoff:
                continue

            events.append({
                "type"   : event.type,
                "reason" : event.reason,
                "message": event.message,
                "object" : event.involved_object.name,
                "count"  : event.count,
            })

        events = sorted(events, key=lambda x: x["count"] or 0, reverse=True)

        summary = f"{len(events)} warning events in last {since}" if events else f"No warning events in last {since}"

        return ok("get_events", {"events": events, "count": len(events)}, summary)

    # ───────────────────────────────────────────────────
    # HELM UPGRADE
    # ───────────────────────────────────────────────────
    def helm_upgrade(self, params: Dict) -> Dict:
        release = params.get("release")
        chart   = params.get("chart", release)
        version = params.get("version")
        ns      = params.get("namespace", self.namespace)

        if not release:
            return err("helm_upgrade", "release param is required")

        cmd = ["helm", "upgrade", release, chart, "-n", ns]
        if version:
            cmd += ["--version", version]

        result = subprocess.run(cmd, capture_output=True, text=True)

        if result.returncode != 0:
            return err("helm_upgrade", result.stderr.strip())

        return ok(
            "helm_upgrade",
            {"release": release, "version": version, "namespace": ns},
            f"{release} upgraded to {version or 'latest'} successfully"
        )

    # ───────────────────────────────────────────────────
    # HELM ROLLBACK
    # ───────────────────────────────────────────────────
    def helm_rollback(self, params: Dict) -> Dict:
        release  = params.get("release")
        revision = params.get("revision", "0")
        ns       = params.get("namespace", self.namespace)

        if not release:
            return err("helm_rollback", "release param is required")

        cmd = ["helm", "rollback", release, str(revision), "-n", ns]
        result = subprocess.run(cmd, capture_output=True, text=True)

        if result.returncode != 0:
            return err("helm_rollback", result.stderr.strip())

        rev_text = f"revision {revision}" if str(revision) != "0" else "previous revision"

        return ok(
            "helm_rollback",
            {"release": release, "revision": revision, "namespace": ns},
            f"{release} rolled back to {rev_text} successfully"
        )

    # ───────────────────────────────────────────────────
    # AUTO DIAGNOSE — Full Incident Context
    # ───────────────────────────────────────────────────
    def auto_diagnose(self, params: Dict) -> Dict:
        service = params.get("target") or params.get("service")
        ns      = params.get("namespace", self.namespace)

        diagnosis = {}
        suggested_actions = []

        # 1. pod status
        pod_result = self.get_pods({"namespace": ns})
        diagnosis["pods"] = pod_result.get("data")

        # 2. failing pods
        fail_result = self.get_failing_pods({"namespace": ns})
        fail_data   = fail_result.get("data", {})
        diagnosis["failures"] = fail_data

        # 3. logs — last 30 min
        if service:
            log_result = self.fetch_logs({"service": service, "namespace": ns, "since": "30m"})
            diagnosis["logs"] = log_result.get("data")

        # 4. recent events
        event_result = self.get_events({"namespace": ns, "since": "1h"})
        diagnosis["events"] = event_result.get("data")

        # 5. resource usage
        usage_result = self.get_resource_usage({"namespace": ns})
        diagnosis["resources"] = usage_result.get("data")

        # 6. suggested actions
        if fail_data.get("crash_loop"):
            suggested_actions.append("rollback — crash loop detected, last deploy may be broken")

        if fail_data.get("oom_killed"):
            suggested_actions.append("increase memory limits — OOMKilled pods detected")

        if fail_data.get("pending"):
            suggested_actions.append("check node capacity — pods are stuck in pending")

        if fail_data.get("failing"):
            suggested_actions.append("check logs — pods in failed state")

        if not suggested_actions:
            suggested_actions.append("no critical issues detected — monitor for 5 minutes")

        # summary
        issues = fail_data.get("total_issues", 0)
        if issues == 0:
            summary = f"Diagnosis complete for {service or 'cluster'} — all healthy"
        else:
            summary = (
                f"Diagnosis for {service or 'cluster'}: "
                f"{issues} issue(s) found. "
                f"Suggested: {suggested_actions[0]}"
            )

        return ok(
            "auto_diagnose",
            {
                "service"          : service,
                "namespace"        : ns,
                "diagnosis"        : diagnosis,
                "suggested_actions": suggested_actions,
                "total_issues"     : issues,
            },
            summary
        )


# ═══════════════════════════════════════════════════════
# HELPER — Parse duration string to seconds
# ═══════════════════════════════════════════════════════
def _parse_duration_to_seconds(duration: str) -> int:
    duration = str(duration).strip()
    if duration.endswith("s"):
        return int(duration[:-1])
    if duration.endswith("m"):
        return int(duration[:-1]) * 60
    if duration.endswith("h"):
        return int(duration[:-1]) * 3600
    try:
        return int(duration)
    except ValueError:
        return 600  # default 10 min


# ═══════════════════════════════════════════════════════
# FACTORY — Create from schema config
# ═══════════════════════════════════════════════════════
def create_connector(config_obj) -> KubernetesConnector:
    connector_cfg = {}
    namespace     = "default"

    if hasattr(config_obj, "connectors") and config_obj.connectors:
        k8s = config_obj.connectors.kubernetes
        if k8s:
            connector_cfg = k8s.dict() if hasattr(k8s, "dict") else {}

    if hasattr(config_obj, "environments"):
        for env_name, env in config_obj.environments.items():
            if hasattr(env, "namespace") and env.namespace:
                namespace = env.namespace
                break

    connector = KubernetesConnector(connector_cfg, namespace)
    connector.connect()
    return connector


# ═══════════════════════════════════════════════════════
# CLI TEST
# ═══════════════════════════════════════════════════════
if __name__ == "__main__":
    print("🔌  Connecting to Kubernetes cluster...")

    connector = KubernetesConnector({}, namespace="default")

    try:
        connector.connect()
        print("✅  Connected\n")

        # health check
        healthy = connector.health_check()
        print(f"🏥  Health: {'OK' if healthy else 'UNREACHABLE'}\n")

        # test get_pods
        result = connector.execute("get_pods", {"namespace": "default"})
        print(f"📦  Pods: {result['summary']}")

        # test get_failing_pods
        result = connector.execute("get_failing_pods", {"namespace": "default"})
        print(f"🚨  Failures: {result['summary']}")

        # test get_events
        result = connector.execute("get_events", {"namespace": "default", "since": "1h"})
        print(f"📋  Events: {result['summary']}")

    except ConnectionError as e:
        print(f"❌  Connection failed: {e}")
        print("    Make sure kubeconfig is configured correctly")
