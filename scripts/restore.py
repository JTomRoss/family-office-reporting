"""
FO Reporting - Sistema RESTORE.

Restaura el sistema desde un snapshot freeze.

Proceso:
1) Verifica git limpio (sin cambios locales sin commit)
2) Detiene app completa con scripts/stop.ps1
3) Hace checkout al tag solicitado
4) Restaura snapshot (DB + raw + cache)
5) Verifica puertos libres (8000/8501)
6) Levanta app completa con scripts/start.ps1
7) Health checks + smoke tests

USO:
    python scripts/restore.py --tag 20260226_120000_cierre_enero_2026
    python scripts/restore.py --latest
"""

import argparse
import shutil
import socket
import subprocess
import sys
import time
from pathlib import Path

import httpx

# Rutas
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
DB_DIR = DATA_DIR / "db"
RAW_DIR = DATA_DIR / "raw"
CACHE_DIR = DATA_DIR / "cache"
SNAPSHOTS_DIR = DATA_DIR / "snapshots"
LATEST_FILE = PROJECT_ROOT / "LATEST_VALID_BACKUP.txt"
SCRIPTS_DIR = PROJECT_ROOT / "scripts"

BACKEND_URL = "http://localhost:8000"
FRONTEND_URL = "http://localhost:8501"
BACKEND_PORT = 8000
FRONTEND_PORT = 8501


def parse_latest() -> dict:
    """Lee LATEST_VALID_BACKUP.txt."""
    if not LATEST_FILE.exists():
        return {}
    content = LATEST_FILE.read_text(encoding="utf-8")
    return dict(
        line.split("=", 1) for line in content.strip().splitlines() if "=" in line
    )


def check_git_clean() -> bool:
    """Verifica que no haya cambios locales sin commit antes de restore."""
    result = subprocess.run(
        ["git", "status", "--porcelain"],
        capture_output=True,
        text=True,
        cwd=str(PROJECT_ROOT),
    )
    if result.returncode != 0:
        print(f"ERROR: git status fallo: {result.stderr}")
        return False
    if result.stdout.strip():
        print("ERROR: Hay cambios sin commit. Haz commit/stash antes de restore.")
        print(result.stdout)
        return False
    return True


def run_ps1(script_name: str) -> bool:
    """Ejecuta un script PowerShell del proyecto."""
    script_path = SCRIPTS_DIR / script_name
    if not script_path.exists():
        print(f"ERROR: Script no encontrado: {script_path}")
        return False

    result = subprocess.run(
        [
            "powershell",
            "-NoProfile",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str(script_path),
        ],
        capture_output=True,
        text=True,
        cwd=str(PROJECT_ROOT),
    )

    if result.stdout:
        print(result.stdout.rstrip())
    if result.stderr:
        print(result.stderr.rstrip())

    if result.returncode != 0:
        print(f"ERROR ejecutando {script_name} (exit {result.returncode})")
        return False
    return True


def tag_exists(tag_name: str) -> bool:
    """Verifica que el tag exista en git."""
    result = subprocess.run(
        ["git", "rev-parse", "--verify", f"refs/tags/{tag_name}"],
        capture_output=True,
        text=True,
        cwd=str(PROJECT_ROOT),
    )
    return result.returncode == 0


def checkout_tag(tag_name: str) -> bool:
    """Checkout del tag git."""
    result = subprocess.run(
        ["git", "checkout", tag_name],
        capture_output=True,
        text=True,
        cwd=str(PROJECT_ROOT),
    )
    if result.returncode != 0:
        print(f"ERROR checkout tag {tag_name}: {result.stderr}")
        return False
    print(f"OK Checkout: {tag_name}")
    return True


def show_head_hash() -> str:
    """Muestra y retorna hash del HEAD."""
    result = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        capture_output=True,
        text=True,
        cwd=str(PROJECT_ROOT),
    )
    git_hash = result.stdout.strip()
    print(f"  HEAD: {git_hash[:12]}")
    return git_hash


def restore_snapshot(tag_name: str) -> bool:
    """Restaura datos desde snapshot."""
    snapshot_dir = SNAPSHOTS_DIR / tag_name
    if not snapshot_dir.exists():
        print(f"ERROR: Snapshot no encontrado: {snapshot_dir}")
        return False

    # Restaurar DB
    db_backup = snapshot_dir / "fo_reporting.db"
    if db_backup.exists():
        DB_DIR.mkdir(parents=True, exist_ok=True)
        db_target = DB_DIR / "fo_reporting.db"
        shutil.copy2(str(db_backup), str(db_target))
        print(f"  OK DB restaurada ({db_backup.stat().st_size / 1024:.1f} KB)")

    # Restaurar raw docs
    raw_archive = snapshot_dir / "raw_docs.zip"
    if raw_archive.exists():
        if RAW_DIR.exists():
            shutil.rmtree(str(RAW_DIR))
        RAW_DIR.mkdir(parents=True, exist_ok=True)
        shutil.unpack_archive(str(raw_archive), str(RAW_DIR))
        print("  OK Raw docs restaurados")

    # Restaurar cache
    cache_archive = snapshot_dir / "cache.zip"
    if cache_archive.exists():
        if CACHE_DIR.exists():
            shutil.rmtree(str(CACHE_DIR))
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        shutil.unpack_archive(str(cache_archive), str(CACHE_DIR))
        print("  OK Cache restaurado")

    return True


def check_port_free(port: int) -> bool:
    """Verifica que el puerto este libre."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex(("localhost", port)) != 0


def verify_ports_free(ports: list[int]) -> bool:
    """Verifica que todos los puertos indicados esten libres."""
    busy = [port for port in ports if not check_port_free(port)]
    if busy:
        print(f"ERROR: Puertos ocupados tras stop: {busy}")
        return False
    print(f"  OK Puertos libres: {ports}")
    return True


def wait_for_url(url: str, timeout: int, label: str) -> bool:
    """Espera a que una URL responda HTTP 200."""
    print(f"  Esperando {label} (max {timeout}s)...", end="", flush=True)
    for _ in range(timeout):
        try:
            resp = httpx.get(url, timeout=2)
            if resp.status_code == 200:
                print(" OK")
                return True
        except Exception:
            pass
        time.sleep(1)
        print(".", end="", flush=True)
    print(" FAIL")
    return False


def smoke_test() -> bool:
    """Ejecuta smoke tests backend + frontend."""
    tests = [
        ("GET backend /", lambda: httpx.get(f"{BACKEND_URL}/", timeout=5)),
        ("GET backend /health", lambda: httpx.get(f"{BACKEND_URL}/api/v1/health", timeout=5)),
        ("GET backend /parsers", lambda: httpx.get(f"{BACKEND_URL}/api/v1/parsers", timeout=5)),
        ("GET backend /accounts", lambda: httpx.get(f"{BACKEND_URL}/api/v1/accounts/", timeout=5)),
        ("GET frontend /", lambda: httpx.get(f"{FRONTEND_URL}/", timeout=5)),
    ]

    all_ok = True
    for name, call in tests:
        try:
            resp = call()
            status = "OK" if resp.status_code == 200 else "FAIL"
            if resp.status_code != 200:
                all_ok = False
            print(f"  {status} {name} -> {resp.status_code}")
        except Exception as exc:
            print(f"  FAIL {name} -> {exc}")
            all_ok = False

    return all_ok


def restore(tag_name: str) -> bool:
    """Proceso completo de restore."""
    print("=" * 60)
    print("  FO REPORTING - RESTORE")
    print("=" * 60)

    # 1) Verificar estado git
    print("\n[1/7] Verificando git limpio...")
    if not check_git_clean():
        return False
    print("  OK Git limpio")

    # 2) Detener app
    print("\n[2/7] Deteniendo app actual...")
    if not run_ps1("stop.ps1"):
        return False
    if not verify_ports_free([BACKEND_PORT, FRONTEND_PORT]):
        return False

    # 3) Checkout tag
    print(f"\n[3/7] Checkout tag: {tag_name}")
    if not tag_exists(tag_name):
        print(f"ERROR: Tag no existe: {tag_name}")
        return False
    if not checkout_tag(tag_name):
        return False
    show_head_hash()

    # 4) Restaurar snapshot
    print("\n[4/7] Restaurando snapshot...")
    if not restore_snapshot(tag_name):
        return False

    # 5) Verificar puertos antes de levantar
    print(f"\n[5/7] Verificando puertos {BACKEND_PORT}/{FRONTEND_PORT}...")
    if not verify_ports_free([BACKEND_PORT, FRONTEND_PORT]):
        return False

    # 6) Iniciar app completa con script oficial
    print("\n[6/7] Iniciando app restaurada...")
    if not run_ps1("start.ps1"):
        return False

    if not wait_for_url(f"{BACKEND_URL}/api/v1/health", timeout=30, label="backend"):
        print("ERROR: Backend no respondio")
        return False
    if not wait_for_url(f"{FRONTEND_URL}/", timeout=30, label="frontend"):
        print("ERROR: Frontend no respondio")
        return False

    # 7) Smoke test
    print("\n[7/7] Smoke tests...")
    if not smoke_test():
        print("WARNING: Algunos smoke tests fallaron")

    print("\n" + "=" * 60)
    print(f"  RESTORE COMPLETADO: {tag_name}")
    print(f"  Backend:  {BACKEND_URL}")
    print(f"  Frontend: {FRONTEND_URL}")
    print("=" * 60)
    return True


def main():
    parser = argparse.ArgumentParser(description="FO Reporting - Restore System")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--tag", "-t", help="Tag del snapshot a restaurar")
    group.add_argument("--latest", action="store_true", help="Restaurar ultimo backup valido")

    args = parser.parse_args()

    if args.latest:
        latest = parse_latest()
        tag = latest.get("TAG")
        if not tag:
            print("ERROR: No hay LATEST_VALID_BACKUP.txt o esta vacio")
            sys.exit(1)
        print(f"Ultimo backup valido: {tag}")
    else:
        tag = args.tag

    success = restore(tag)
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
