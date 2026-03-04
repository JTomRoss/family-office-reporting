"""
FO Reporting – FastAPI Application (entrypoint).

Este es el ÚNICO entrypoint del backend.
Ejecutar:
    uvicorn backend.main:app --host 0.0.0.0 --port 8000
"""

from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from backend.config import get_settings, ensure_dirs
from backend.db.init_db import init_database
from backend.routers import health, documents, accounts, data


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup: inicializa directorios y BD."""
    ensure_dirs()
    init_database()
    yield


settings = get_settings()

app = FastAPI(
    title=settings.app_name,
    version=settings.app_version,
    description="Sistema de reporting financiero interno para Family Office",
    lifespan=lifespan,
)

# ── CORS (orígenes configurables desde .env) ────────────────────
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Registrar routers ───────────────────────────────────────────
app.include_router(health.router, prefix=settings.api_prefix)
app.include_router(documents.router, prefix=settings.api_prefix)
app.include_router(accounts.router, prefix=settings.api_prefix)
app.include_router(data.router, prefix=settings.api_prefix)


@app.get("/")
def root():
    """Redirect a docs."""
    return {
        "app": settings.app_name,
        "version": settings.app_version,
        "docs": "/docs",
        "api_prefix": settings.api_prefix,
    }
