"""Conexión a PostgreSQL: de dónde salen los datos de acceso.

Usa las mismas variables que docker-compose.yml (POSTGRES_USER,
POSTGRES_PASSWORD, POSTGRES_DB, POSTGRES_PORT) más POSTGRES_HOST.
Prioridad: variable de entorno > archivo .env de la raíz del repo > valor por
defecto. Así, si se cambian las credenciales en .env, el contenedor y los
scripts de Python quedan de acuerdo sin tocar código.
"""
import os
from pathlib import Path
from typing import Mapping

import psycopg

from happiness_etl.config import ROOT_DIR

ENV_FILE = ROOT_DIR / ".env"

# variable -> (parámetro de conexión, valor por defecto; los mismos del docker-compose.yml)
_SETTINGS = {
    "POSTGRES_HOST": ("host", "127.0.0.1"),
    "POSTGRES_PORT": ("port", "5432"),
    "POSTGRES_DB": ("dbname", "happiness"),
    "POSTGRES_USER": ("user", "happiness"),
    "POSTGRES_PASSWORD": ("password", "happiness"),
}


def read_env_file(path: Path = ENV_FILE) -> dict[str, str]:
    """Lee un archivo .env sencillo (CLAVE=valor por línea). Si no existe, devuelve {}."""
    values: dict[str, str] = {}
    if not path.is_file():
        return values
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
            value = value[1:-1]
        values[key.strip()] = value
    return values


def connection_settings(
    environ: Mapping[str, str] | None = None, env_file: Path = ENV_FILE
) -> dict[str, str]:
    """Parámetros de conexión para psycopg.connect, ya resueltos."""
    environ = os.environ if environ is None else environ
    from_file = read_env_file(env_file)
    return {
        parameter: environ.get(variable) or from_file.get(variable) or default
        for variable, (parameter, default) in _SETTINGS.items()
    }


def connect(**overrides) -> psycopg.Connection:
    """Abre una conexión a la base del proyecto (falla en 10 s si no responde)."""
    return psycopg.connect(**{**connection_settings(), "connect_timeout": 10, **overrides})
