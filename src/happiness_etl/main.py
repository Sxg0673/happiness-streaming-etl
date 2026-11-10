"""Orquestador del ETL de la Parte A.

Orden: extract -> clean -> transform -> validate (compuerta) -> load -> reconciliación.
Uso: python -m happiness_etl.main
Sale con código 0 si todo salió bien y 1 si la validación detuvo el proceso.
"""
import logging
import sys
from pathlib import Path

from happiness_etl.clean import clean_all
from happiness_etl.config import PROCESSED_FILE
from happiness_etl.extract import read_all
from happiness_etl.load import read_processed, write_processed
from happiness_etl.transform import transform
from happiness_etl.validate import ValidationError, validate_dataset, validate_load

logger = logging.getLogger("happiness_etl")


def run(output: Path = PROCESSED_FILE) -> Path:
    """Ejecuta el ETL completo y devuelve la ruta del archivo escrito."""
    logger.info("1/5 extract")
    raw = read_all()
    logger.info("2/5 clean")
    cleaned = clean_all(raw)
    logger.info("3/5 transform")
    unified = transform(cleaned)
    logger.info("4/5 validate")
    validate_dataset(unified)  # si falla, no se escribe nada
    logger.info("5/5 load")
    path = write_processed(unified, output)
    validate_load(unified, read_processed(path))
    return path


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-7s %(name)s: %(message)s")
    try:
        path = run()
    except ValidationError as exc:
        logger.error("ETL detenido por la compuerta de calidad.\n%s", exc)
        return 1
    logger.info("ETL terminado: %s", path)
    return 0


if __name__ == "__main__":
    sys.exit(main())
