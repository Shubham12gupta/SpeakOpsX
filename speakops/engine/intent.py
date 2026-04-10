# Talk your Infra

# RAW INPUT
# SANITIZE
# NORMALIZE
# INTENT MATCH
# PARAMETER EXTRACTION
# VALIDATION
# CONFIDENCE
# STRUCTURED OUTPUT

#1 RAW INPUT
# x = input("Talk to your infra")   #Scale the payment-service to 5 replicas ++ add a voice library
#2 SANITIZE
#scale payment-service to 5!!!!! -> 5
#3 NORMALIZATION
#Increase payment-service pods to 5 -> scale payment-service to 5
#4 LOAD YAML INTENT
#voice.config.yaml loaded
#5 Intent matching
# x == voice.config.yaml
#6 Parameter extraction
# after extracting write in a json
#7 Strict validation
# conditional statement like replicas <= max_limit
#8 Intent whitelisting
# if operation not in yaml_defined_operations:
#     reject()
#9 Drift detection
# check user input ==  parsed intent
# like user input "check logs" but parsed "restart service" reject it
#10 Confidence score
# after drift detection match this and give a score like - exact match = high, fuzzy match = medium, unclear = reject
#11 Structured Output
# {
#     "connector": "kubernetes",
#     "operation": "scale",
#     "params": {
#         "service": "payment-service"
#         "replicas": 5
#      },
#      "confidence": 0.95
# }
import re
import json
import yaml
import difflib
from typing import Dict, Any, Optional


# -------------------------------
# CONFIG LOAD
# -------------------------------
def load_config(path: str = "voice.config.yaml") -> Dict:
    with open(path, "r") as f:
        return yaml.safe_load(f)


# -------------------------------
# SANITIZE INPUT
# -------------------------------
def sanitize(text: str) -> str:
    text = text.lower().strip()
    text = re.sub(r"[^\w\s\-]", "", text)
    return text


# -------------------------------
# NORMALIZATION
# -------------------------------
SYNONYMS = {
    "increase"  : "scale",
    "decrease"  : "scale",
    "reduce"    : "scale",
    "bump"      : "scale",
    "pods"      : "replicas",
    "instances" : "replicas",
    "containers": "replicas",
    "bring up"  : "scale",
    "bring down": "scale",
    "reboot"    : "restart",
    "redeploy"  : "restart",
    "bounce"    : "restart",
    "undo"      : "rollback",
    "revert"    : "rollback",
    "go back"   : "rollback",
    "trigger"   : "deploy",
    "push"      : "deploy",
    "ship"      : "deploy",
    "whats"     : "what is",
    "logs"      : "show logs",
    "status"    : "show pod status",
}

def normalize(text: str) -> str:
    for phrase, replacement in sorted(SYNONYMS.items(), key=lambda x: -len(x[0])):
        text = text.replace(phrase, replacement)
    return text


# -------------------------------
# GENERIC VARIABLE EXTRACTOR
# -------------------------------
VARIABLE_PATTERNS = {
    "service"     : r"(?P<service>[\w\-]+)",
    "replicas"    : r"(?P<replicas>\d+)",
    "branch"      : r"(?P<branch>[\w\-\/]+)",
    "environment" : r"(?P<environment>dev|staging|production|prod)",
    "duration"    : r"(?P<duration>\d+[smh])",
    "app"         : r"(?P<app>[\w\-]+)",
    "release"     : r"(?P<release>[\w\-]+)",
    "version"     : r"(?P<version>[\w\.\-]+)",
    "revision"    : r"(?P<revision>\d+)",
    "job"         : r"(?P<job>[\w\-]+)",
    "dashboard"   : r"(?P<dashboard>[\w\-]+)",
}

def build_pattern(template: str) -> str:
    pattern = re.escape(template)
    for var, regex in VARIABLE_PATTERNS.items():
        escaped_var = re.escape("{" + var + "}")
        pattern = pattern.replace(escaped_var, regex)
    return f"^{pattern}$"


# -------------------------------
# MATCH ACROSS ALL CONNECTORS
# -------------------------------
def match_all_connectors(text: str, config: Dict) -> Optional[Dict]:
    all_commands = config.get("voiceCommands", {})

    for connector_name, commands in all_commands.items():
        if not isinstance(commands, dict):
            continue

        for command_name, command in commands.items():
            if not isinstance(command, dict):
                continue

            intents = command.get("intent", [])
            for template in intents:
                try:
                    pattern = build_pattern(template)
                    match = re.match(pattern, text)
                    if match:
                        return {
                            "connector"      : command.get("connector", connector_name),
                            "operation"      : command.get("operation"),
                            "params"         : match.groupdict(),
                            "template"       : template,
                            "command_name"   : command_name,
                            "command_def"    : command,
                            "source"         : "regex",
                        }
                except re.error:
                    continue

    return None


# -------------------------------
# LLM FALLBACK
# -------------------------------
def llm_parse(user_input: str, config: Dict) -> Optional[Dict]:
    try:
        import anthropic

        all_intents = []
        all_commands = config.get("voiceCommands", {})

        for connector_name, commands in all_commands.items():
            if not isinstance(commands, dict):
                continue
            for command_name, command in commands.items():
                if not isinstance(command, dict):
                    continue
                for intent in command.get("intent", []):
                    all_intents.append({
                        "connector"   : command.get("connector", connector_name),
                        "operation"   : command.get("operation"),
                        "intent"      : intent,
                        "command_name": command_name,
                    })

        system_prompt = f"""You are SpeakOps intent parser for a DevOps orchestration tool.

Your job is to map user voice input to one of the defined intents below.

Available intents:
{json.dumps(all_intents, indent=2)}

Rules:
- Return ONLY valid JSON, no explanation, no markdown
- Extract all variable values from user input
- If no match found, return {{"match": false}}
- Variables to extract: service, replicas, branch, environment, duration, app, release, version, revision, job, dashboard

Return format:
{{
  "match": true,
  "connector": "kubernetes",
  "operation": "scale",
  "command_name": "scaleDeployment",
  "intent": "scale {{service}} to {{replicas}} replicas",
  "params": {{
    "service": "payment",
    "replicas": "5"
  }}
}}"""

        client = anthropic.Anthropic()
        response = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=500,
            system=system_prompt,
            messages=[{"role": "user", "content": user_input}]
        )

        raw = response.content[0].text.strip()
        result = json.loads(raw)

        if not result.get("match"):
            return None

        all_commands_flat = config.get("voiceCommands", {})
        command_def = None
        for connector_cmds in all_commands_flat.values():
            if isinstance(connector_cmds, dict):
                cmd = connector_cmds.get(result.get("command_name", ""))
                if cmd:
                    command_def = cmd
                    break

        return {
            "connector"   : result.get("connector"),
            "operation"   : result.get("operation"),
            "params"      : result.get("params", {}),
            "template"    : result.get("intent", ""),
            "command_name": result.get("command_name"),
            "command_def" : command_def,
            "source"      : "llm",
        }

    except Exception:
        return None


# -------------------------------
# PARAM VALIDATION — GENERIC
# -------------------------------
def validate_params(params: Dict, command_def: Dict) -> Dict:
    validated = {}
    safety = command_def.get("safety", {})

    for key, value in params.items():
        if value is None:
            continue

        if key == "replicas":
            replicas = int(value)
            max_replicas = safety.get("max_replicas", 20)
            block_zero = safety.get("block_scale_to_zero", True)

            if replicas <= 0 and block_zero:
                raise ValueError("Scaling to 0 replicas is blocked by safety config")
            if replicas > max_replicas:
                raise ValueError(f"Replicas {replicas} exceed max limit of {max_replicas}")

            validated[key] = replicas

        elif key == "environment":
            allowed_envs = ["dev", "development", "staging", "production", "prod"]
            if value not in allowed_envs:
                raise ValueError(f"Unknown environment: {value}")
            validated[key] = value

        else:
            validated[key] = value

    return validated


# -------------------------------
# CONFIDENCE SCORE — MULTI FACTOR
# -------------------------------
def compute_confidence(user_input: str, template: str, params: Dict, source: str) -> float:
    if source == "llm":
        return 0.80

    template_clean = re.sub(r"\{[\w]+\}", "", template).strip()
    template_words = set(template_clean.split())
    input_words = set(user_input.split())

    if not template_words:
        word_score = 0.5
    else:
        common = len(input_words & template_words)
        word_score = common / len(template_words)

    param_score = 1.0 if params else 0.5

    confidence = round((word_score * 0.7) + (param_score * 0.3), 2)
    return min(confidence, 1.0)


# -------------------------------
# SUGGEST CLOSEST INTENT
# -------------------------------
def suggest_closest(text: str, config: Dict) -> Optional[str]:
    all_intents = []
    all_commands = config.get("voiceCommands", {})

    for commands in all_commands.values():
        if isinstance(commands, dict):
            for command in commands.values():
                if isinstance(command, dict):
                    for intent in command.get("intent", []):
                        all_intents.append(intent)

    matches = difflib.get_close_matches(text, all_intents, n=1, cutoff=0.3)
    return matches[0] if matches else None


# -------------------------------
# RBAC CHECK
# -------------------------------
def check_rbac(command_def: Dict, user_role: str) -> bool:
    allowed_roles = command_def.get("rbac", {}).get("allowed_roles", [])
    if not allowed_roles:
        return True
    return user_role in allowed_roles


# -------------------------------
# CONFIRM CHECK
# -------------------------------
def needs_confirmation(command_def: Dict, environment: str) -> bool:
    confirm_in = command_def.get("execution", {}).get("confirm_in", [])
    return environment in confirm_in


# -------------------------------
# MAIN PARSER
# -------------------------------
def parse_intent(
    user_input: str,
    environment: str = "staging",
    user_role: str = "senior_engineer"
) -> Dict:

    config = load_config()

    # 1. sanitize
    clean = sanitize(user_input)

    # 2. normalize
    normalized = normalize(clean)

    # 3. regex match — all connectors
    match = match_all_connectors(normalized, config)

    # 4. LLM fallback
    if not match:
        match = llm_parse(user_input, config)

    # 5. no match — suggest closest
    if not match:
        suggestion = suggest_closest(normalized, config)
        error_msg = "No matching intent found."
        if suggestion:
            error_msg += f" Did you mean: '{suggestion}'?"
        raise ValueError(error_msg)

    command_def = match.get("command_def") or {}

    # 6. RBAC check
    if not check_rbac(command_def, user_role):
        raise PermissionError(
            f"Role '{user_role}' is not allowed to run '{match['operation']}'"
        )

    # 7. validate params
    validated_params = validate_params(match["params"], command_def)

    # 8. confidence
    confidence = compute_confidence(
        normalized,
        match["template"],
        validated_params,
        match["source"]
    )

    if confidence < 0.4:
        suggestion = suggest_closest(normalized, config)
        error_msg = f"Low confidence ({confidence}). Input unclear."
        if suggestion:
            error_msg += f" Did you mean: '{suggestion}'?"
        raise ValueError(error_msg)

    # 9. confirm check
    confirm = needs_confirmation(command_def, environment)

    # 10. structured output
    return {
        "connector"      : match["connector"],
        "operation"      : match["operation"],
        "params"         : validated_params,
        "confidence"     : confidence,
        "source"         : match["source"],
        "raw_input"      : user_input,
        "matched_intent" : match["template"],
        "needs_confirm"  : confirm,
        "environment"    : environment,
        "user_role"      : user_role,
        "allowed_roles"  : command_def.get("rbac", {}).get("allowed_roles", []),
        "audit"          : command_def.get("audit", {}),
        "timeout"        : command_def.get("execution", {}).get("timeout", 60),
    }


# -------------------------------
# CLI TEST
# -------------------------------
if __name__ == "__main__":
    try:
        user_input = input("🎙️  Talk to your infra: ")
        env = input("🌍  Environment (dev/staging/production): ") or "staging"
        role = input("👤  Your role (junior_engineer/senior_engineer/admin): ") or "senior_engineer"

        result = parse_intent(user_input, environment=env, user_role=role)

        print("\n✅  Parsed Intent:")
        for key, value in result.items():
            print(f"    {key:<20} : {value}")

    except PermissionError as e:
        print(f"\n🔒  Access Denied: {str(e)}")

    except ValueError as e:
        print(f"\n❌  Error: {str(e)}")

























































