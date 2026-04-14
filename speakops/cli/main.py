import sys
import json
import datetime
import pathlib

import click
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.text import Text
from rich import box

# ─── Internal imports ─────────────────────────────────
sys.path.insert(0, str(pathlib.Path(__file__).parent.parent))

from config.schema import load_and_validate, get_all_intents
from engine.intent import parse_intent
from security.engine import security_check
from connectors.kubernetes import KubernetesConnector

console = Console()

SPEAKOPS_VERSION = "v1.0.0"
AUDIT_LOG_PATH   = pathlib.Path.home() / ".speakops" / "audit.jsonl"
ENV_FILE_PATH    = pathlib.Path.home() / ".speakops" / "active_env"
CONFIG_PATH      = "voice.config.yaml"


# ═══════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════

def print_banner():
    console.print(Panel(
        Text.assemble(
            ("SpeakOps ", "bold green"),
            (SPEAKOPS_VERSION, "dim green"),
            ("\nTalk to your infrastructure. It listens.", "italic dim white"),
        ),
        border_style="green",
        padding=(0, 2),
    ))


def startup() -> object:
    try:
        config = load_and_validate(CONFIG_PATH)
        return config
    except FileNotFoundError:
        console.print(
            "\n[bold red]✗[/bold red] [red]voice.config.yaml not found[/red]"
        )
        console.print("[dim]  Run [bold]speakops init[/bold] to create one.[/dim]\n")
        sys.exit(1)
    except ValueError as e:
        console.print(
            f"\n[bold red]✗[/bold red] [red]Config validation failed:[/red]\n{e}\n"
        )
        sys.exit(1)


def get_active_env(config) -> str:
    if ENV_FILE_PATH.exists():
        saved = ENV_FILE_PATH.read_text().strip()
        if saved in config.environments:
            return saved
    return list(config.environments.keys())[0]


def save_active_env(env: str):
    ENV_FILE_PATH.parent.mkdir(parents=True, exist_ok=True)
    ENV_FILE_PATH.write_text(env)


def write_audit(intent: dict, decision: dict, result: dict):
    AUDIT_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    entry = {
        "timestamp"      : datetime.datetime.utcnow().isoformat() + "Z",
        "raw_input"      : intent.get("raw_input"),
        "operation"      : intent.get("operation"),
        "connector"      : intent.get("connector"),
        "params"         : intent.get("params"),
        "environment"    : intent.get("environment"),
        "user_role"      : intent.get("user_role"),
        "confidence"     : intent.get("confidence"),
        "risk_level"     : decision.get("summary", {}).get("risk_level"),
        "approved"       : decision.get("approved"),
        "success"        : result.get("success") if result else None,
        "summary"        : result.get("summary") if result else None,
    }
    with open(AUDIT_LOG_PATH, "a") as f:
        f.write(json.dumps(entry) + "\n")


def execute_connector(intent: dict, config) -> dict:
    connector_name = intent.get("connector")
    operation      = intent.get("operation")
    params         = intent.get("params", {})

    if connector_name == "kubernetes":
        ns = "default"
        env_name = intent.get("environment", "staging")
        env_cfg  = config.environments.get(env_name)
        if env_cfg and env_cfg.namespace:
            ns = env_cfg.namespace

        k8s_cfg = {}
        if config.connectors and config.connectors.kubernetes:
            k8s_cfg = config.connectors.kubernetes.dict()

        connector = KubernetesConnector(k8s_cfg, namespace=ns)
        connector.connect()
        return connector.execute(operation, params)

    return {
        "success": False,
        "summary": f"Connector '{connector_name}' not yet implemented in V1",
        "error"  : f"Connector '{connector_name}' coming in V2",
    }


def capture_voice_text() -> str:
    try:
        import faster_whisper
        import pyaudio
        import wave
        import tempfile
        import numpy as np

        RATE    = 16000
        CHUNK   = 1024
        SECONDS = 5

        pa     = pyaudio.PyAudio()
        stream = pa.open(
            format=pyaudio.paInt16, channels=1,
            rate=RATE, input=True, frames_per_buffer=CHUNK
        )

        frames = []
        for _ in range(int(RATE / CHUNK * SECONDS)):
            frames.append(stream.read(CHUNK))

        stream.stop_stream()
        stream.close()
        pa.terminate()

        with tempfile.NamedTemporaryFile(suffix=".wav", delete=False) as f:
            wf = wave.open(f.name, "wb")
            wf.setnchannels(1)
            wf.setsampwidth(pa.get_sample_size(pyaudio.paInt16))
            wf.setframerate(RATE)
            wf.writeframes(b"".join(frames))
            wf.close()

            model = faster_whisper.WhisperModel("base", device="cpu")
            segments, _ = model.transcribe(f.name, language="en")
            text = " ".join(seg.text.strip() for seg in segments)
            return text.strip()

    except ImportError:
        console.print(
            "[yellow]⚠[/yellow] [dim]Voice libraries not installed. "
            "Using text input fallback.[/dim]"
        )
        return console.input("[bold green]❯[/bold green] Type command: ").strip()


# ═══════════════════════════════════════════════════════
# CLI GROUP
# ═══════════════════════════════════════════════════════
@click.group()
@click.version_option(SPEAKOPS_VERSION, "--version", "-v", message="SpeakOps %(version)s")
def cli():
    """
    \b
    SpeakOps — Voice-driven DevOps orchestration.
    Talk to your infrastructure. It listens.
    """
    pass


# ═══════════════════════════════════════════════════════
# COMMAND — listen
# ═══════════════════════════════════════════════════════
@cli.command()
@click.option("--env",  default=None,              help="Environment to target")
@click.option("--role", default="senior_engineer", help="Your RBAC role")
def listen(env, role):
    """Start voice listening mode."""

    print_banner()
    config     = startup()
    active_env = env or get_active_env(config)

    console.print(
        f"\n[bold green]✓[/bold green] Listening in "
        f"[bold]{active_env}[/bold] as [bold]{role}[/bold]\n"
        f"[dim]  Press Ctrl+C to stop.[/dim]\n"
    )

    while True:
        try:
            with console.status("[green]Listening...[/green]", spinner="dots"):
                text = capture_voice_text()

            if not text:
                console.print("[dim]  Nothing heard — try again[/dim]")
                continue

            console.print(f'\n[blue]🎙  Heard:[/blue] [bold]"{text}"[/bold]')

            with console.status("[green]Understanding...[/green]", spinner="dots"):
                intent = parse_intent(text, environment=active_env, user_role=role)

            with console.status("[green]Security check...[/green]", spinner="dots"):
                decision = security_check(intent)

            if not decision.get("approved") and not decision.get("needs_confirmation"):
                console.print(
                    f"[bold red]✗ Blocked:[/bold red] {decision.get('rejection_reason')}"
                )
                write_audit(intent, decision, None)
                continue

            if decision.get("needs_confirmation"):
                console.print(
                    f"\n[bold yellow]⚠  Confirm required:[/bold yellow] "
                    + ", ".join(decision.get("confirmation_reasons", []))
                )
                console.print('[dim]  Say "confirm" or "cancel"[/dim]')

                with console.status("[yellow]Waiting for confirmation...[/yellow]", spinner="dots"):
                    confirm_text = capture_voice_text()

                if "confirm" not in confirm_text.lower():
                    console.print("[red]✗ Cancelled[/red]\n")
                    continue

            if decision.get("dry_run"):
                console.print("\n[dim][DRY RUN] Would execute:[/dim]")
                console.print_json(json.dumps(decision.get("preview", {})))
                continue

            summary = decision.get("summary", {})
            console.print(
                f"[dim]  Risk: [bold]{summary.get('risk_level', 'unknown')}[/bold]  "
                f"Blast: [bold]{summary.get('blast_radius', 'unknown')}[/bold][/dim]"
            )

            with console.status(
                f"[green]Executing {intent.get('operation')}...[/green]",
                spinner="dots"
            ):
                result = execute_connector(intent, config)

            if result.get("success"):
                console.print(f"[bold green]✓[/bold green] {result.get('summary')}\n")
            else:
                console.print(f"[bold red]✗[/bold red] {result.get('error')}\n")

            write_audit(intent, decision, result)

        except KeyboardInterrupt:
            console.print("\n\n[dim]SpeakOps stopped.[/dim]\n")
            break

        except PermissionError as e:
            console.print(f"[bold red]✗ Access Denied:[/bold red] {e}\n")

        except ValueError as e:
            console.print(f"[bold red]✗ Error:[/bold red] {e}\n")

        except Exception as e:
            console.print(f"[bold red]✗ Unexpected:[/bold red] {e}\n")


# ═══════════════════════════════════════════════════════
# COMMAND — run
# ═══════════════════════════════════════════════════════
@cli.command()
@click.argument("text")
@click.option("--env",  default=None,              help="Environment to target")
@click.option("--role", default="senior_engineer", help="Your RBAC role")
@click.option("--dry",  is_flag=True,              help="Dry run — preview only")
def run(text, env, role, dry):
    """Run a command by text instead of voice."""

    config     = startup()
    active_env = env or get_active_env(config)

    console.print(f'\n[blue]❯[/blue] [bold]"{text}"[/bold]\n')

    try:
        with console.status("[green]Understanding...[/green]", spinner="dots"):
            intent = parse_intent(text, environment=active_env, user_role=role)

        with console.status("[green]Security check...[/green]", spinner="dots"):
            decision = security_check(intent)

        if dry or decision.get("dry_run"):
            console.print("[bold yellow][DRY RUN][/bold yellow]")
            _print_decision(intent, decision)
            return

        if not decision.get("approved") and not decision.get("needs_confirmation"):
            console.print(
                f"[bold red]✗ Blocked:[/bold red] {decision.get('rejection_reason')}\n"
            )
            return

        if decision.get("needs_confirmation"):
            console.print(
                "[bold yellow]⚠  Confirmation required:[/bold yellow] "
                + ", ".join(decision.get("confirmation_reasons", []))
            )
            confirmed = click.confirm("  Proceed?")
            if not confirmed:
                console.print("[red]✗ Cancelled[/red]\n")
                return

        with console.status(
            f"[green]Executing {intent.get('operation')}...[/green]",
            spinner="dots"
        ):
            result = execute_connector(intent, config)

        if result.get("success"):
            console.print(f"[bold green]✓[/bold green] {result.get('summary')}\n")
        else:
            console.print(f"[bold red]✗[/bold red] {result.get('error')}\n")

        write_audit(intent, decision, result)

    except PermissionError as e:
        console.print(f"[bold red]✗ Access Denied:[/bold red] {e}\n")

    except ValueError as e:
        console.print(f"[bold red]✗ Error:[/bold red] {e}\n")


# ═══════════════════════════════════════════════════════
# COMMAND — validate
# ═══════════════════════════════════════════════════════
@cli.command()
def validate():
    """Validate voice.config.yaml."""

    console.print("\n[bold]Validating voice.config.yaml...[/bold]\n")

    try:
        config = load_and_validate(CONFIG_PATH)

        console.print("[bold green]✓ Config is valid[/bold green]\n")

        table = Table(box=box.SIMPLE, show_header=False, padding=(0, 2))
        table.add_column("Key",   style="dim")
        table.add_column("Value", style="bold white")

        table.add_row("Project",      config.meta.project_name)
        table.add_row("Version",      config.meta.version)
        table.add_row("Owner",        config.meta.owner_email or "—")
        table.add_row("Environments", ", ".join(config.environments.keys()))

        connectors = []
        if config.connectors:
            if config.connectors.kubernetes: connectors.append("kubernetes")
            if config.connectors.jenkins:    connectors.append("jenkins")
            if config.connectors.argocd:     connectors.append("argocd")
            if config.connectors.grafana:    connectors.append("grafana")
        table.add_row("Connectors", ", ".join(connectors) or "—")

        intents = get_all_intents(config)
        table.add_row("Total intents", str(len(intents)))

        console.print(table)

    except FileNotFoundError:
        console.print(
            "[bold red]✗ File not found:[/bold red] voice.config.yaml\n"
            "[dim]  Run [bold]speakops init[/bold] to create one.[/dim]\n"
        )

    except ValueError as e:
        console.print(f"[bold red]✗ Validation failed:[/bold red]\n{e}\n")


# ═══════════════════════════════════════════════════════
# COMMAND — status
# ═══════════════════════════════════════════════════════
@cli.command()
def status():
    """Check connector and cluster health."""

    config     = startup()
    active_env = get_active_env(config)

    console.print(f"\n[bold]SpeakOps Status[/bold]  [dim]{SPEAKOPS_VERSION}[/dim]\n")
    console.print(f"Active environment: [bold green]{active_env}[/bold green]\n")

    table = Table(box=box.SIMPLE, padding=(0, 2))
    table.add_column("Connector", style="bold white")
    table.add_column("Status")
    table.add_column("Details", style="dim")

    if config.connectors:

        if config.connectors.kubernetes:
            try:
                k8s = KubernetesConnector({}, namespace="default")
                k8s.connect()
                healthy = k8s.health_check()
                if healthy:
                    table.add_row("kubernetes", "[green]✓ Connected[/green]", "Cluster reachable")
                else:
                    table.add_row("kubernetes", "[red]✗ Unreachable[/red]", "Cluster not responding")
            except Exception as e:
                table.add_row("kubernetes", "[red]✗ Error[/red]", str(e)[:50])

        if config.connectors.jenkins:
            table.add_row("jenkins", "[yellow]— Skipped[/yellow]", "V2 connector")

        if config.connectors.argocd:
            table.add_row("argocd", "[yellow]— Skipped[/yellow]", "V2 connector")

        if config.connectors.grafana:
            table.add_row("grafana", "[yellow]— Skipped[/yellow]", "V2 connector")

    console.print(table)


# ═══════════════════════════════════════════════════════
# COMMAND — enroll
# ═══════════════════════════════════════════════════════
@cli.command()
def enroll():
    """Enroll your voice profile for biometric authentication."""

    console.print(Panel(
        "[bold green]Voice Enrollment[/bold green]\n"
        "[dim]You will speak 3 times to build your voice profile.\n"
        "Say the same phrase each time:[/dim]\n\n"
        '[bold white]"Authorize SpeakOps access"[/bold white]',
        border_style="green", padding=(1, 2)
    ))

    if not click.confirm("\n  Ready to start?"):
        console.print("[dim]Enrollment cancelled.[/dim]\n")
        return

    try:
        import numpy as np
        import librosa
        import pickle
        import cryptography.fernet as fernet

        samples = []

        for i in range(1, 4):
            console.print(f"\n[bold]Sample {i}/3[/bold] — speak now")

            with console.status(f"[green]Recording sample {i}...[/green]", spinner="dots"):
                text = capture_voice_text()

            console.print(f"[green]✓[/green] Sample {i} recorded")
            samples.append(text)

        with console.status("[green]Building voice profile...[/green]", spinner="moon"):
            profile_path = pathlib.Path.home() / ".speakops" / "voice.enc"
            profile_path.parent.mkdir(parents=True, exist_ok=True)

            key  = fernet.Fernet.generate_key()
            f    = fernet.Fernet(key)
            data = pickle.dumps({"samples": samples, "enrolled_at": datetime.datetime.utcnow().isoformat()})
            encrypted = f.encrypt(data)

            profile_path.write_bytes(encrypted)
            key_path = pathlib.Path.home() / ".speakops" / "voice.key"
            key_path.write_bytes(key)

        console.print("\n[bold green]✓ Enrollment complete[/bold green]")
        console.print(f"[dim]  Profile saved to {profile_path}[/dim]\n")

    except ImportError:
        console.print(
            "\n[yellow]⚠[/yellow] Voice libraries not installed.\n"
            "[dim]  pip install librosa pyaudio faster-whisper cryptography[/dim]\n"
        )

    except Exception as e:
        console.print(f"\n[bold red]✗ Enrollment failed:[/bold red] {e}\n")


# ═══════════════════════════════════════════════════════
# COMMAND — audit
# ═══════════════════════════════════════════════════════
@cli.command()
@click.option("--last", default=10,  help="Show last N entries", type=int)
@click.option("--env",  default=None, help="Filter by environment")
def audit(last, env):
    """View command history and audit log."""

    if not AUDIT_LOG_PATH.exists():
        console.print("\n[dim]No audit logs found yet.[/dim]\n")
        return

    with open(AUDIT_LOG_PATH, "r") as f:
        lines = f.readlines()

    entries = []
    for line in lines:
        try:
            entry = json.loads(line.strip())
            if env and entry.get("environment") != env:
                continue
            entries.append(entry)
        except json.JSONDecodeError:
            continue

    entries = entries[-last:]

    if not entries:
        console.print("\n[dim]No matching audit entries.[/dim]\n")
        return

    console.print(f"\n[bold]Audit Log[/bold] [dim](last {len(entries)} entries)[/dim]\n")

    table = Table(box=box.SIMPLE, padding=(0, 1))
    table.add_column("Time",        style="dim",        no_wrap=True)
    table.add_column("Operation",   style="bold white")
    table.add_column("Params",      style="dim")
    table.add_column("Env",         style="cyan")
    table.add_column("Risk",        style="yellow")
    table.add_column("Status")

    for e in entries:
        ts        = e.get("timestamp", "")[:19].replace("T", " ")
        operation = e.get("operation", "—")
        params    = str(e.get("params", {}))[:30]
        env_name  = e.get("environment", "—")
        risk      = e.get("risk_level", "—")
        success   = e.get("success")

        if success is True:
            status = "[green]✓ OK[/green]"
        elif success is False:
            status = "[red]✗ Failed[/red]"
        else:
            status = "[yellow]— Pending[/yellow]"

        table.add_row(ts, operation, params, env_name, risk, status)

    console.print(table)


# ═══════════════════════════════════════════════════════
# COMMAND — env
# ═══════════════════════════════════════════════════════
@cli.group()
def env():
    """Manage active environment."""
    pass


@env.command("use")
@click.argument("environment")
def env_use(environment):
    """Switch active environment."""

    config = startup()

    if environment not in config.environments:
        console.print(
            f"\n[bold red]✗[/bold red] Unknown environment: [bold]{environment}[/bold]\n"
            f"[dim]  Available: {', '.join(config.environments.keys())}[/dim]\n"
        )
        return

    save_active_env(environment)
    console.print(
        f"\n[bold green]✓[/bold green] Switched to [bold]{environment}[/bold]\n"
    )


@env.command("show")
def env_show():
    """Show active environment."""

    config     = startup()
    active_env = get_active_env(config)
    env_cfg    = config.environments.get(active_env)

    console.print(f"\n[bold]Active environment:[/bold] [bold green]{active_env}[/bold green]\n")

    if env_cfg:
        table = Table(box=box.SIMPLE, show_header=False, padding=(0, 2))
        table.add_column("Key",   style="dim")
        table.add_column("Value", style="bold white")
        table.add_row("Namespace",    env_cfg.namespace or "default")
        table.add_row("Auto confirm", str(env_cfg.auto_confirm))
        table.add_row("Restrictions", str(env_cfg.restrictions))
        console.print(table)


# ═══════════════════════════════════════════════════════
# COMMAND — dry-run
# ═══════════════════════════════════════════════════════
@cli.command("dry-run")
@click.argument("text")
@click.option("--env",  default=None,              help="Environment to target")
@click.option("--role", default="senior_engineer", help="Your RBAC role")
def dry_run(text, env, role):
    """Preview what a command would do without executing."""

    config     = startup()
    active_env = env or get_active_env(config)

    console.print(f'\n[yellow][DRY RUN][/yellow] [bold]"{text}"[/bold]\n')

    try:
        with console.status("[green]Parsing...[/green]", spinner="dots"):
            intent = parse_intent(text, environment=active_env, user_role=role)

        with console.status("[green]Evaluating...[/green]", spinner="dots"):
            decision = security_check(intent)

        _print_decision(intent, decision)

    except PermissionError as e:
        console.print(f"[bold red]✗ Would be blocked:[/bold red] {e}\n")

    except ValueError as e:
        console.print(f"[bold red]✗ Would fail:[/bold red] {e}\n")


# ═══════════════════════════════════════════════════════
# COMMAND — init
# ═══════════════════════════════════════════════════════
@cli.command()
def init():
    """Initialize SpeakOps in the current project."""

    config_path = pathlib.Path(CONFIG_PATH)

    if config_path.exists():
        if not click.confirm(
            "[yellow]voice.config.yaml already exists. Overwrite?[/yellow]"
        ):
            console.print("[dim]Init cancelled.[/dim]\n")
            return

    template = """\
# ─────────────────────────────────────────────
#  SpeakOps Configuration
#  voice.config.yaml
# ─────────────────────────────────────────────

meta:
  project_name: my-app
  version: v1
  owner_email: engineer@company.com
  description: "SpeakOps config for my-app"

environments:
  staging:
    namespace: staging
    auto_confirm: true
    restrictions: false
  production:
    namespace: production
    auto_confirm: false
    restrictions: true

connectors:
  kubernetes:
    type: eks
    cluster: my-cluster.ap-south-1.eksctl.io
    auth: kubeconfig
    api_key: ${KUBE_API_KEY}

rbac:
  junior_engineer:
    access: read
    allowed_operations: [podStatus, fetchLogs]
    denied_operations: [scaleDeployment, rollback]
  senior_engineer:
    access: read_write
    allowed_operations: ["*"]
    require_confirm_for: [scaleDeployment, rollback]
  admin:
    access: read_write
    allowed_operations: ["*"]
    bypass_confirm: false

voiceCommands:
  kubernetes:
    scaleDeployment:
      intent:
        - "scale {service} to {replicas} replicas"
        - "scale up {service} to {replicas}"
      connector: kubernetes
      operation: scale
      params:
        target: "deployment/{service}"
        replicas: "{replicas}"
        namespace: "default"
      execution:
        mode: execute
        confirm_in: [production]
        timeout: 60
      safety:
        blast_radius_check: true
        max_replicas: 20
      rbac:
        allowed_roles: [senior_engineer, admin]
      audit:
        log: true
        level: info

    podStatus:
      intent:
        - "show pod status"
        - "get all pods"
      connector: kubernetes
      operation: get_pods
      params:
        namespace: "default"
        output: voice_summary
      execution:
        mode: read
        confirm_in: []
        timeout: 30
      safety:
        blast_radius_check: false
      rbac:
        allowed_roles: [junior_engineer, senior_engineer, admin]
      audit:
        log: true
        level: info

safety:
  dry_run_mode: false
  blast_radius_check: true
  max_replicas_voice: 20
  forbidden_operations: []

security:
  voice_auth:
    enabled: true
    enrollment_samples: 3
    similarity_threshold: 0.85

audit:
  backend: local
  retention_days: 90

notifications:
  on_success:
    voice_response: true
  on_failure:
    voice_response: true
"""

    config_path.write_text(template)

    console.print("\n[bold green]✓ SpeakOps initialized[/bold green]\n")
    console.print("[dim]  voice.config.yaml created.[/dim]")
    console.print("[dim]  Edit it with your cluster details.[/dim]")
    console.print("[dim]  Then run: [bold]speakops validate[/bold][/dim]\n")


# ═══════════════════════════════════════════════════════
# HELPER — Print decision summary
# ═══════════════════════════════════════════════════════
def _print_decision(intent: dict, decision: dict):
    summary = decision.get("summary", {})

    table = Table(box=box.SIMPLE, show_header=False, padding=(0, 2))
    table.add_column("Field", style="dim")
    table.add_column("Value", style="bold white")

    table.add_row("Connector",    intent.get("connector", "—"))
    table.add_row("Operation",    intent.get("operation", "—"))
    table.add_row("Params",       str(intent.get("params", {})))
    table.add_row("Environment",  intent.get("environment", "—"))
    table.add_row("Confidence",   str(intent.get("confidence", "—")))
    table.add_row("Source",       intent.get("source", "—"))
    table.add_row("Risk level",   summary.get("risk_level", "—"))
    table.add_row("Blast radius", summary.get("blast_radius", "—"))
    table.add_row("Needs confirm",str(decision.get("needs_confirmation", False)))
    table.add_row("Approved",     str(decision.get("approved", False)))

    warnings = summary.get("warnings", [])
    if warnings:
        table.add_row("Warnings", "\n".join(f"⚠ {w}" for w in warnings))

    console.print(table)
    console.print("[dim]  Nothing was executed.[/dim]\n")


# ═══════════════════════════════════════════════════════
# ENTRY POINT
# ═══════════════════════════════════════════════════════
if __name__ == "__main__":
    cli()