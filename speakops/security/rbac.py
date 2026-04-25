import pathlib
import yaml
from typing import Dict, List, Optional


# ═══════════════════════════════════════════════════════
# LOAD CONFIG
# ═══════════════════════════════════════════════════════
def load_config(path: str = None) -> Dict:
    if path is None:
        path = str(pathlib.Path(__file__).resolve().parent.parent / "voice.config.yaml")
    with open(path, "r") as f:
        return yaml.safe_load(f)


# ═══════════════════════════════════════════════════════
# ROLE DEFINITIONS
# ═══════════════════════════════════════════════════════
DEFAULT_ROLES = {
    "junior_engineer": {
        "access"              : "read",
        "allowed_operations"  : ["get_pods", "fetch_logs", "get_resource_usage",
                                  "get_failing_pods", "get_events", "get_pipeline_status",
                                  "get_app_health", "get_out_of_sync", "fetch_metrics",
                                  "get_alerts", "open_dashboard", "auto_diagnose"],
        "denied_operations"   : ["scale", "rollout_restart", "rollout_undo",
                                  "helm_upgrade", "helm_rollback", "trigger_build",
                                  "trigger_deploy", "cancel_pipeline", "sync_app",
                                  "rollback_app"],
        "require_confirm_for" : [],
        "bypass_confirm"      : False,
    },
    "senior_engineer": {
        "access"              : "read_write",
        "allowed_operations"  : ["*"],
        "denied_operations"   : [],
        "require_confirm_for" : ["scale", "rollout_undo", "helm_upgrade",
                                  "helm_rollback", "trigger_deploy", "rollback_app"],
        "bypass_confirm"      : False,
    },
    "admin": {
        "access"              : "read_write",
        "allowed_operations"  : ["*"],
        "denied_operations"   : [],
        "require_confirm_for" : ["rollout_undo", "helm_rollback", "rollback_app"],
        "bypass_confirm"      : False,
    },
}


# ═══════════════════════════════════════════════════════
# RBAC ENGINE
# ═══════════════════════════════════════════════════════
class RBACEngine:

    def __init__(self, config: Dict = None):
        self.config = config or load_config()
        self.roles  = self._load_roles()

    def _load_roles(self) -> Dict:
        rbac_config = self.config.get("rbac", {})
        roles       = {}

        for role_name, role_def in rbac_config.items():
            if not isinstance(role_def, dict):
                continue
            roles[role_name] = {
                "access"              : role_def.get("access", "read"),
                "allowed_operations"  : role_def.get("allowed_operations", []),
                "denied_operations"   : role_def.get("denied_operations", []),
                "require_confirm_for" : role_def.get("require_confirm_for", []),
                "bypass_confirm"      : role_def.get("bypass_confirm", False),
            }

        # merge with defaults for missing roles
        for role_name, default_def in DEFAULT_ROLES.items():
            if role_name not in roles:
                roles[role_name] = default_def

        return roles

    # ───────────────────────────────────────────────────
    # CHECK ACCESS
    # ───────────────────────────────────────────────────
    def check_access(self, user_role: str, operation: str) -> Dict:
        role_def = self.roles.get(user_role)

        if not role_def:
            return {
                "allowed"       : False,
                "reason"        : f"Unknown role: {user_role}",
                "needs_confirm" : False,
            }

        allowed_ops = role_def.get("allowed_operations", [])
        denied_ops  = role_def.get("denied_operations", [])
        confirm_ops = role_def.get("require_confirm_for", [])

        # denied check first
        if operation in denied_ops:
            return {
                "allowed"       : False,
                "reason"        : f"Operation '{operation}' is explicitly denied for role '{user_role}'",
                "needs_confirm" : False,
            }

        # allowed check
        if "*" not in allowed_ops and operation not in allowed_ops:
            return {
                "allowed"       : False,
                "reason"        : f"Role '{user_role}' does not have access to '{operation}'",
                "needs_confirm" : False,
            }

        # confirm check
        needs_confirm = operation in confirm_ops

        return {
            "allowed"       : True,
            "reason"        : "Access granted",
            "needs_confirm" : needs_confirm,
            "role"          : user_role,
            "access"        : role_def.get("access"),
            "bypass_confirm": role_def.get("bypass_confirm", False),
        }

    # ───────────────────────────────────────────────────
    # GET ROLE INFO
    # ───────────────────────────────────────────────────
    def get_role(self, user_role: str) -> Optional[Dict]:
        return self.roles.get(user_role)

    # ───────────────────────────────────────────────────
    # LIST ALL ROLES
    # ───────────────────────────────────────────────────
    def list_roles(self) -> List[str]:
        return list(self.roles.keys())

    # ───────────────────────────────────────────────────
    # GET ALLOWED OPERATIONS FOR ROLE
    # ───────────────────────────────────────────────────
    def get_allowed_operations(self, user_role: str) -> List[str]:
        role_def = self.roles.get(user_role, {})
        return role_def.get("allowed_operations", [])

    # ───────────────────────────────────────────────────
    # VALIDATE ROLE EXISTS
    # ───────────────────────────────────────────────────
    def role_exists(self, user_role: str) -> bool:
        return user_role in self.roles


# ═══════════════════════════════════════════════════════
# CONVENIENCE FUNCTIONS
# ═══════════════════════════════════════════════════════
_engine: Optional[RBACEngine] = None

def get_engine(config: Dict = None) -> RBACEngine:
    global _engine
    if _engine is None or config is not None:
        _engine = RBACEngine(config)
    return _engine

def check_access(user_role: str, operation: str, config: Dict = None) -> Dict:
    return get_engine(config).check_access(user_role, operation)

def is_allowed(user_role: str, operation: str, config: Dict = None) -> bool:
    return get_engine(config).check_access(user_role, operation).get("allowed", False)

def needs_confirmation(user_role: str, operation: str, config: Dict = None) -> bool:
    result = get_engine(config).check_access(user_role, operation)
    return result.get("needs_confirm", False)


# ═══════════════════════════════════════════════════════
# CLI TEST
# ═══════════════════════════════════════════════════════
if __name__ == "__main__":
    print("\n🔐  RBAC Engine Test\n")

    engine = RBACEngine()

    test_cases = [
        ("junior_engineer", "get_pods"),
        ("junior_engineer", "scale"),
        ("junior_engineer", "fetch_logs"),
        ("senior_engineer", "scale"),
        ("senior_engineer", "rollout_undo"),
        ("admin",           "helm_rollback"),
        ("admin",           "scale"),
        ("unknown_role",    "get_pods"),
    ]

    for role, operation in test_cases:
        result = engine.check_access(role, operation)
        status = "✅ ALLOWED" if result["allowed"] else "❌ DENIED"
        confirm = " [CONFIRM NEEDED]" if result.get("needs_confirm") else ""
        print(f"  {status}{confirm}")
        print(f"  Role: {role} | Operation: {operation}")
        print(f"  Reason: {result['reason']}\n")