"""Pruebas de db: de dónde salen los datos de conexión (no necesita una base de datos)."""
import re

from happiness_etl import config, db

DEFAULTS = {"host": "127.0.0.1", "port": "5432", "dbname": "happiness", "user": "happiness", "password": "happiness"}


def test_defaults_when_nothing_is_configured(tmp_path):
    assert db.connection_settings(environ={}, env_file=tmp_path / "no-existe.env") == DEFAULTS


def test_env_file_overrides_defaults(tmp_path):
    env_file = tmp_path / ".env"
    env_file.write_text(
        "# comentario\n\nPOSTGRES_USER=ana\nPOSTGRES_PASSWORD = 'se=creto'\nPOSTGRES_PORT=\"5544\"\nlinea sin igual\n",
        encoding="utf-8",
    )
    settings = db.connection_settings(environ={}, env_file=env_file)
    assert settings == {**DEFAULTS, "user": "ana", "password": "se=creto", "port": "5544"}


def test_environment_overrides_env_file(tmp_path):
    env_file = tmp_path / ".env"
    env_file.write_text("POSTGRES_USER=ana\nPOSTGRES_DB=otra\n", encoding="utf-8")
    settings = db.connection_settings(environ={"POSTGRES_USER": "luis", "POSTGRES_HOST": "db.local"}, env_file=env_file)
    assert (settings["user"], settings["dbname"], settings["host"]) == ("luis", "otra", "db.local")


def test_empty_values_fall_back_to_the_default(tmp_path):
    env_file = tmp_path / ".env"
    env_file.write_text("POSTGRES_USER=\n", encoding="utf-8")
    assert db.connection_settings(environ={"POSTGRES_DB": ""}, env_file=env_file) == DEFAULTS


def test_defaults_match_docker_compose_and_env_example():
    """Si alguien cambia un valor por defecto en un lado, debe cambiarlo en los tres."""
    compose = (config.ROOT_DIR / "docker-compose.yml").read_text(encoding="utf-8")
    in_compose = dict(re.findall(r"\$\{(POSTGRES_\w+):-([^}]+)\}", compose))
    example = db.read_env_file(config.ROOT_DIR / ".env.example")
    expected = {"POSTGRES_USER": "happiness", "POSTGRES_PASSWORD": "happiness", "POSTGRES_DB": "happiness", "POSTGRES_PORT": "5432"}
    assert in_compose == expected and example == expected
    settings = db.connection_settings(environ={}, env_file=config.ROOT_DIR / ".env.example")
    assert settings == DEFAULTS
