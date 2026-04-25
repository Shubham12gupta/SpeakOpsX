import json
import pathlib
import datetime
import hashlib
from typing import Dict, Any, Optional, List


# ═══════════════════════════════════════════════════════
# PATHS
# ═══════════════════════════════════════════════════════
AUDIT_DIR  = pathlib.Path.home() / ".speakopsx" / "audit"
AUDIT_FILE = AUDIT_DIR / "commands.jsonl"


# ═══════════════════════════════════════════════════════
# AUDIT LOGGER
# ═══════════════════════════════════════════════════════
class AuditLogger:

    def __init__(self, log_path: pathlib.Path = AUDIT_FILE):
        self.log_path = log_path
        self.log_path.parent.mkdir(parents=True, exist_ok=True)

    # ───────────────────────────────────────────────────
    # LOG COMMAND
    # ───────────────────────────────────────────────────
    def log(
        self,
        intent    : Dict,
        decision  : Dict,
        result    : Optional[Dict] = None,
        extra     : Optional[Dict] = None,
    ) -> str:

        now        = datetime.datetime.now(datetime.timezone.utc)
        request_id = decision.get("request_id") or self._generate_id(intent, now)

        entry = {
            "request_id"     : request_id,
            "timestamp"      : now.isoformat(),
            "raw_input"      : intent.get("raw_input", ""),
            "matched_intent" : intent.get("matched_intent", ""),
            "connector"      : intent.get("connector", ""),
            "operation"      : intent.get("operation", ""),
            "params"         : intent.get("params", {}),
            "environment"    : intent.get("environment", ""),
            "user_role"      : intent.get("user_role", ""),
            "confidence"     : intent.get("confidence", 0),
            "source"         : intent.get("source", ""),
            "risk_level"     : decision.get("summary", {}).get("risk_level", ""),
            "blast_radius"   : decision.get("summary", {}).get("blast_radius", ""),
            "approved"       : decision.get("approved", False),
            "needs_confirm"  : decision.get("needs_confirmation", False),
            "dry_run"        : decision.get("dry_run", False),
            "success"        : result.get("success") if result else None,
            "result_summary" : result.get("summary") if result else None,
            "error"          : result.get("error") if result else None,
        }

        if extra:
            entry.update(extra)

        with open(self.log_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry) + "\n")

        return request_id

    # ───────────────────────────────────────────────────
    # LOG SECURITY EVENT
    # ───────────────────────────────────────────────────
    def log_security_event(
        self,
        event_type : str,
        user_role  : str,
        operation  : str,
        reason     : str,
        environment: str = "",
    ) -> None:

        entry = {
            "request_id" : self._generate_id({"operation": operation}, datetime.datetime.now(datetime.timezone.utc)),
            "timestamp"  : datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "event_type" : f"SECURITY_{event_type.upper()}",
            "user_role"  : user_role,
            "operation"  : operation,
            "environment": environment,
            "reason"     : reason,
            "approved"   : False,
        }

        with open(self.log_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry) + "\n")

    # ───────────────────────────────────────────────────
    # READ LOGS
    # ───────────────────────────────────────────────────
    def read(
        self,
        last        : int = 20,
        environment : Optional[str] = None,
        operation   : Optional[str] = None,
        user_role   : Optional[str] = None,
        success_only: bool = False,
    ) -> List[Dict]:

        if not self.log_path.exists():
            return []

        entries = []
        with open(self.log_path, "r", encoding="utf-8") as f:
            for line in f:
                try:
                    entry = json.loads(line.strip())
                    entries.append(entry)
                except json.JSONDecodeError:
                    continue

        # filters
        if environment:
            entries = [e for e in entries if e.get("environment") == environment]
        if operation:
            entries = [e for e in entries if e.get("operation") == operation]
        if user_role:
            entries = [e for e in entries if e.get("user_role") == user_role]
        if success_only:
            entries = [e for e in entries if e.get("success") is True]

        return entries[-last:]

    # ───────────────────────────────────────────────────
    # GET STATS
    # ───────────────────────────────────────────────────
    def get_stats(self) -> Dict:
        entries = self.read(last=10000)

        if not entries:
            return {"total": 0, "message": "No audit logs found"}

        total     = len(entries)
        success   = sum(1 for e in entries if e.get("success") is True)
        failed    = sum(1 for e in entries if e.get("success") is False)
        blocked   = sum(1 for e in entries if e.get("approved") is False)
        dry_runs  = sum(1 for e in entries if e.get("dry_run") is True)

        # top operations
        op_counts: Dict[str, int] = {}
        for e in entries:
            op = e.get("operation", "unknown")
            op_counts[op] = op_counts.get(op, 0) + 1

        top_ops = sorted(op_counts.items(), key=lambda x: x[1], reverse=True)[:5]

        # top users
        role_counts: Dict[str, int] = {}
        for e in entries:
            role = e.get("user_role", "unknown")
            role_counts[role] = role_counts.get(role, 0) + 1

        return {
            "total"      : total,
            "success"    : success,
            "failed"     : failed,
            "blocked"    : blocked,
            "dry_runs"   : dry_runs,
            "top_ops"    : top_ops,
            "role_counts": role_counts,
        }

    # ───────────────────────────────────────────────────
    # CLEAR LOGS
    # ───────────────────────────────────────────────────
    def clear(self, confirm: bool = False) -> bool:
        if not confirm:
            return False
        if self.log_path.exists():
            self.log_path.unlink()
        return True

    # ───────────────────────────────────────────────────
    # EXPORT LOGS
    # ───────────────────────────────────────────────────
    def export(self, output_path: str, fmt: str = "json") -> str:
        entries = self.read(last=100000)
        path    = pathlib.Path(output_path)

        if fmt == "json":
            with open(path, "w", encoding="utf-8") as f:
                json.dump(entries, f, indent=2)
        elif fmt == "csv":
            import csv
            if entries:
                with open(path, "w", newline="", encoding="utf-8") as f:
                    writer = csv.DictWriter(f, fieldnames=entries[0].keys())
                    writer.writeheader()
                    writer.writerows(entries)

        return str(path)

    # ───────────────────────────────────────────────────
    # GENERATE REQUEST ID
    # ───────────────────────────────────────────────────
    def _generate_id(self, intent: Dict, now: datetime.datetime) -> str:
        raw = f"{intent.get('operation', '')}{now.isoformat()}"
        return hashlib.sha256(raw.encode()).hexdigest()[:12]


# ═══════════════════════════════════════════════════════
# GLOBAL INSTANCE
# ═══════════════════════════════════════════════════════
_logger: Optional[AuditLogger] = None

def get_logger() -> AuditLogger:
    global _logger
    if _logger is None:
        _logger = AuditLogger()
    return _logger

def log_command(intent: Dict, decision: Dict, result: Optional[Dict] = None) -> str:
    return get_logger().log(intent, decision, result)

def log_security_event(event_type: str, user_role: str, operation: str, reason: str, environment: str = "") -> None:
    get_logger().log_security_event(event_type, user_role, operation, reason, environment)


# ═══════════════════════════════════════════════════════
# CLI TEST
# ═══════════════════════════════════════════════════════
if __name__ == "__main__":
    print("\n📋  Audit Logger Test\n")

    logger = AuditLogger()

    # mock intent + decision + result
    mock_intent = {
        "connector"      : "kubernetes",
        "operation"      : "scale",
        "params"         : {"service": "payment", "replicas": 5},
        "confidence"     : 0.95,
        "source"         : "regex",
        "raw_input"      : "scale payment to 5 replicas",
        "matched_intent" : "scale {service} to {replicas} replicas",
        "environment"    : "staging",
        "user_role"      : "senior_engineer",
    }

    mock_decision = {
        "request_id"        : "abc123def456",
        "approved"          : True,
        "needs_confirmation": False,
        "dry_run"           : False,
        "summary"           : {"risk_level": "medium", "blast_radius": "low"},
    }

    mock_result = {
        "success": True,
        "summary": "payment scaled to 5 replicas",
        "error"  : None,
    }

    req_id = logger.log(mock_intent, mock_decision, mock_result)
    print(f"✅  Logged with request_id: {req_id}")

    entries = logger.read(last=5)
    print(f"📖  Last {len(entries)} entries:")
    for e in entries:
        print(f"    {e['timestamp'][:19]} | {e['operation']} | {e.get('user_role')} | {'✓' if e.get('success') else '✗'}")

    stats = logger.get_stats()
    print(f"\n📊  Stats:")
    print(f"    Total    : {stats['total']}")
    print(f"    Success  : {stats['success']}")
    print(f"    Failed   : {stats['failed']}")
    print(f"    Blocked  : {stats['blocked']}")