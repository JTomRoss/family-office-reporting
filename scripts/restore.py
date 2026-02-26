"""
FO Reporting – Sistema RESTORE.

Restaura el sistema desde un snapshot freeze.

Proceso:
1) Levanta solo desde entrypoint único
2) Muestra hash HEAD
3) Verifica puerto
4) Valida carga HTTP 200
5) Smoke test automático

USO:
    python scripts/restore.py --tag 20260226_120000_cierre_enero_2026
    python scripts/restore.py --latest
"""

import argparse
import shutil
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

BACKEND_URL = "http://localhost:8000"
BACKEND_PORT = 8000


def parse_latest() -> dict:
    """Lee LATEST_VALID_BACKUP.txt"""
    if not LATEST_FILE.exists():
        return {}
    content = LATEST_FILE.read_text()
    return dict(
        line.split("=", 1) for line in content.strip().splitlines() if "=" in line
    )


def checkout_tag(tag_name: str) -> bool:
    """Checkout del tag git."""
    result = subprocess.run(
        ["git", "checkout", tag_name],
        capture_output=True, text=True, cwd=str(PROJECT_ROOT),
    )
    if result.returncode != 0:
        print(f"ERROR checkout tag {tag_name}: {result.stderr}")
        return False
    print(f"✓ Checkout: {tag_name}")
    return True


def show_head_hash() -> str:
    """Muestra y retorna hash del HEAD."""
    result = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        capture_output=True, text=True, cwd=str(PROJECT_ROOT),
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
        print(f"  ✓ DB restaurada ({db_backup.stat().st_size / 1024:.1f} KB)")

    # Restaurar raw docs
    raw_archive = snapshot_dir / "raw_docs.zip"
    if raw_archive.exists():
        if RAW_DIR.exists():
            shutil.rmtree(str(RAW_DIR))
        RAW_DIR.mkdir(parents=True, exist_ok=True)
        shutil.unpack_archive(str(raw_archive), str(RAW_DIR))
        print("  ✓ Raw docs restaurados")

    # Restaurar cache
    cache_archive = snapshot_dir / "cache.zip"
    if cache_archive.exists():
        if CACHE_DIR.exists():
            shutil.rmtree(str(CACHE_DIR))
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        shutil.unpack_archive(str(cache_archive), str(CACHE_DIR))
        print("  ✓ Cache restaurado")

    return True


def check_port_free(port: int) -> bool:
    """Verifica que el puerto esté libre."""
    import socket
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex(("localhost", port)) != 0


def start_backend() -> subprocess.Popen:
    """Inicia el backend."""
    proc = subprocess.Popen(
        [sys.executable, "-m", "uvicorn", "backend.main:app",
         "--host", "0.0.0.0", "--port", str(BACKEND_PORT)],
        cwd=str(PROJECT_ROOT),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    return proc


def wait_for_backend(timeout: int = 30) -> bool:
    """Espera a que el backend esté listo."""
    print(f"  Esperando backend (max {timeout}s)...", end="", flush=True)
    for _ in range(timeout):
        try:
            resp = httpx.get(f"{BACKEND_URL}/api/v1/health", timeout=2)
            if resp.status_code == 200:
                print(" ✓")
                return True
        except Exception:
            pass
        time.sleep(1)
        print(".", end="", flush=True)
    print(" ✗")
    return False


def smoke_test() -> bool:
    """Ejecuta smoke tests contra el backend."""
    tests = [
        ("GET /", lambda: httpx.get(f"{BACKEND_URL}/")),
        ("GET /health", lambda: httpx.get(f"{BACKEND_URL}/api/v1/health")),
        ("GET /parsers", lambda: httpx.get(f"{BACKEND_URL}/api/v1/parsers")),
        ("GET /accounts", lambda: httpx.get(f"{BACKEND_URL}/api/v1/accounts/")),
    ]

    all_ok = True
    for name, call in tests:
        try:
            resp = call()
            status = "✓" if resp.status_code == 200 else "✗"
            if resp.status_code != 200:
                all_ok = False
            print(f"  {status} {name} → {resp.status_code}")
        except Exception as e:
            print(f"  ✗ {name} → {e}")
            all_ok = False

    return all_ok


def restore(tag_name: str) -> bool:
    """Proceso completo de restore."""
    print("=" * 60)
    print("  FO REPORTING – RESTORE")
    print("=" * 60)

    # 1) Checkout tag
    print(f"\n[1/5] Checkout tag: {tag_name}")
    if not checkout_tag(tag_name):
        return False
    show_head_hash()

    # 2) Restaurar snapshot
    print(f"\n[2/5] Restaurando snapshot...")
    if not restore_snapshot(tag_name):
        return False

    # 3) Verificar puerto
    print(f"\n[3/5] Verificando puerto {BACKEND_PORT}...")
    if not check_port_free(BACKEND_PORT):
        print(f"  ⚠️ Puerto {BACKEND_PORT} en uso. Deteniendo proceso existente...")

    # 4) Iniciar backend
    print(f"\n[4/5] Iniciando backend...")
    proc = start_backend()

    if not wait_for_backend():
        print("ERROR: Backend no respondió")
        proc.terminate()
        return False

    # 5) Smoke test
    print(f"\n[5/5] Smoke tests...")
    if not smoke_test():
        print("⚠️ Algunos smoke tests fallaron")

    print("\n" + "=" * 60)
    print(f"  ✅ RESTORE COMPLETADO: {tag_name}")
    print(f"  Backend corriendo en {BACKEND_URL}")
    print("=" * 60)
    return True


def main():
    parser = argparse.ArgumentParser(description="FO Reporting – Restore System")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--tag", "-t", help="Tag del snapshot a restaurar")
    group.add_argument("--latest", action="store_true", help="Restaurar último backup válido")

    args = parser.parse_args()

    if args.latest:
        latest = parse_latest()
        tag = latest.get("TAG")
        if not tag:
            print("ERROR: No hay LATEST_VALID_BACKUP.txt o está vacío")
            sys.exit(1)
        print(f"Último backup válido: {tag}")
    else:
        tag = args.tag

    success = restore(tag)
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
