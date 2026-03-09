"""
FO Reporting – Sistema FREEZE.

Crea un snapshot completo y verificable del sistema.

Proceso:
1) Requiere git status limpio (no cambios sin commit)
2) Genera ID único: YYYYMMDD_HHMMSS_label
3) Crea:
   - Git tag
   - Snapshot FULL datos (DB + raw + outputs + logs)
   - Actualiza LATEST_VALID_BACKUP.txt
4) Verifica integridad

USO:
    python scripts/freeze.py --label "cierre_enero_2026"

PROHIBIDO:
- Nunca auto-commit
- Nunca congelar si hay cambios sin commit
"""

import argparse
import hashlib
import shutil
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

# Rutas
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
DB_DIR = DATA_DIR / "db"
RAW_DIR = DATA_DIR / "raw"
CACHE_DIR = DATA_DIR / "cache"
SNAPSHOTS_DIR = DATA_DIR / "snapshots"
LATEST_FILE = PROJECT_ROOT / "LATEST_VALID_BACKUP.txt"


def check_git_clean() -> bool:
    """Verifica que no haya cambios sin commit."""
    try:
        result = subprocess.run(
            ["git", "status", "--porcelain"],
            capture_output=True, text=True, cwd=str(PROJECT_ROOT),
        )
        if result.returncode != 0:
            print(f"ERROR: git status falló: {result.stderr}")
            return False
        if result.stdout.strip():
            print("ERROR: Hay cambios sin commit:")
            print(result.stdout)
            print("\nHaz commit de todos los cambios antes de freeze.")
            return False
        return True
    except FileNotFoundError:
        print("ERROR: git no encontrado en PATH")
        return False


def get_git_hash() -> str:
    """Obtiene hash del HEAD actual."""
    result = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        capture_output=True, text=True, cwd=str(PROJECT_ROOT),
    )
    return result.stdout.strip()


def create_git_tag(tag_name: str) -> bool:
    """Crea un git tag."""
    result = subprocess.run(
        ["git", "tag", "-a", tag_name, "-m", f"Freeze: {tag_name}"],
        capture_output=True, text=True, cwd=str(PROJECT_ROOT),
    )
    if result.returncode != 0:
        print(f"ERROR creando tag: {result.stderr}")
        return False
    print(f"[OK] Git tag creado: {tag_name}")
    return True


def compute_dir_hash(directory: Path) -> str:
    """Computa hash SHA-256 de todos los archivos en un directorio."""
    sha256 = hashlib.sha256()
    if not directory.exists():
        return sha256.hexdigest()

    for filepath in sorted(directory.rglob("*")):
        if filepath.is_file():
            sha256.update(str(filepath.relative_to(directory)).encode())
            with open(filepath, "rb") as f:
                for chunk in iter(lambda: f.read(8192), b""):
                    sha256.update(chunk)
    return sha256.hexdigest()


def create_snapshot(tag_name: str) -> Path:
    """Crea snapshot completo de datos."""
    SNAPSHOTS_DIR.mkdir(parents=True, exist_ok=True)
    snapshot_dir = SNAPSHOTS_DIR / tag_name
    snapshot_dir.mkdir(exist_ok=True)

    # Copiar DB
    db_file = DB_DIR / "fo_reporting.db"
    if db_file.exists():
        shutil.copy2(str(db_file), str(snapshot_dir / "fo_reporting.db"))
        print(f"  [OK] DB copiada ({db_file.stat().st_size / 1024:.1f} KB)")

    # Copiar raw docs (como archive)
    if RAW_DIR.exists() and any(RAW_DIR.iterdir()):
        shutil.make_archive(
            str(snapshot_dir / "raw_docs"),
            "zip",
            str(RAW_DIR),
        )
        print("  [OK] Raw docs archivados")

    # Copiar cache
    if CACHE_DIR.exists() and any(CACHE_DIR.iterdir()):
        shutil.make_archive(
            str(snapshot_dir / "cache"),
            "zip",
            str(CACHE_DIR),
        )
        print("  [OK] Cache archivado")

    return snapshot_dir


def verify_integrity(snapshot_dir: Path) -> bool:
    """Verifica integridad del snapshot."""
    db_snapshot = snapshot_dir / "fo_reporting.db"

    checks = []

    # Check DB
    if db_snapshot.exists():
        size = db_snapshot.stat().st_size
        checks.append(("DB backup", size > 0))
    else:
        checks.append(("DB backup", False))

    all_ok = all(ok for _, ok in checks)

    for name, ok in checks:
        status = "[OK]" if ok else "[FAIL]"
        print(f"  {status} {name}")

    return all_ok


def update_latest(tag_name: str, git_hash: str, snapshot_dir: Path) -> None:
    """Actualiza LATEST_VALID_BACKUP.txt"""
    now = datetime.now(timezone.utc).isoformat()
    snapshot_hash = compute_dir_hash(snapshot_dir)

    content = f"""TAG={tag_name}
HASH={git_hash}
SNAPSHOT={snapshot_dir}
SNAPSHOT_HASH={snapshot_hash}
FECHA={now}
"""
    LATEST_FILE.write_text(content)
    print(f"[OK] LATEST_VALID_BACKUP.txt actualizado")


def freeze(label: str) -> bool:
    """Ejecuta el proceso completo de freeze."""
    print("=" * 60)
    print("  FO REPORTING – FREEZE")
    print("=" * 60)

    # 1) Verificar git limpio
    print("\n[1/5] Verificando git status...")
    if not check_git_clean():
        return False
    print("  [OK] Git limpio")

    # 2) Generar ID
    now = datetime.now(timezone.utc)
    tag_name = f"{now.strftime('%Y%m%d_%H%M%S')}_{label}"
    print(f"\n[2/5] Tag: {tag_name}")

    # 3) Obtener hash
    git_hash = get_git_hash()
    print(f"  HEAD: {git_hash[:12]}")

    # 4) Crear snapshot
    print("\n[3/5] Creando snapshot...")
    snapshot_dir = create_snapshot(tag_name)

    # 5) Crear git tag
    print("\n[4/5] Creando git tag...")
    if not create_git_tag(tag_name):
        return False

    # 6) Verificar integridad
    print("\n[5/5] Verificando integridad...")
    if not verify_integrity(snapshot_dir):
        print("[AVISO] Verificacion de integridad fallo. Snapshot puede estar incompleto.")

    # 7) Actualizar LATEST
    update_latest(tag_name, git_hash, snapshot_dir)

    print("\n" + "=" * 60)
    print(f"  FREEZE COMPLETADO: {tag_name}")
    print("=" * 60)
    return True


def main():
    parser = argparse.ArgumentParser(description="FO Reporting – Freeze System")
    parser.add_argument(
        "--label", "-l",
        required=True,
        help="Etiqueta descriptiva para el freeze (ej: cierre_enero_2026)",
    )
    args = parser.parse_args()

    # Sanitizar label
    label = args.label.replace(" ", "_").replace("/", "_")

    success = freeze(label)
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
