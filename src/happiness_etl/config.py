"""Configuración del proyecto: rutas, años y nombres canónicos.

Es la única fuente de verdad de los nombres de columnas. Aquí no hay lógica
ni lectura de archivos; el resto de módulos importan estas constantes.
"""
from pathlib import Path

# Raíz del repositorio: src/happiness_etl/config.py -> se suben tres niveles.
# Supone que el paquete se usa instalado en modo editable (pip install -e .).
ROOT_DIR = Path(__file__).resolve().parents[2]
RAW_DIR = ROOT_DIR / "data" / "raw"
PROCESSED_DIR = ROOT_DIR / "data" / "processed"
# Dataset unificado que escribe main.py y leen el entrenamiento y el producer
PROCESSED_FILE = PROCESSED_DIR / "happiness_unified.csv"

YEARS = (2015, 2016, 2017, 2018, 2019)

# Nombres canónicos. Coinciden con los del contrato del evento, salvo el objetivo:
# el evento lo publica como "actual_happiness_score" (lo renombra el producer).
COUNTRY = "country"
YEAR = "year"
FEATURES = ("gdp", "family", "health", "freedom", "generosity", "corruption")
TARGET = "happiness_score"
UNIFIED_COLUMNS = (COUNTRY, YEAR, *FEATURES, TARGET)

# Equivalencias: nombre original en cada archivo -> nombre canónico.
# Las columnas que no aparecen aquí (rangos, errores, intervalos, whiskers,
# Dystopia Residual, Region) no entran al esquema unificado.
_MAP_2015_2016 = {
    "Country": COUNTRY,
    "Happiness Score": TARGET,
    "Economy (GDP per Capita)": "gdp",
    "Family": "family",
    "Health (Life Expectancy)": "health",
    "Freedom": "freedom",
    "Generosity": "generosity",
    "Trust (Government Corruption)": "corruption",
}
_MAP_2017 = {
    "Country": COUNTRY,
    "Happiness.Score": TARGET,
    "Economy..GDP.per.Capita.": "gdp",
    "Family": "family",
    "Health..Life.Expectancy.": "health",
    "Freedom": "freedom",
    "Generosity": "generosity",
    "Trust..Government.Corruption.": "corruption",
}
_MAP_2018_2019 = {
    "Country or region": COUNTRY,
    "Score": TARGET,
    "GDP per capita": "gdp",
    "Social support": "family",
    "Healthy life expectancy": "health",
    "Freedom to make life choices": "freedom",
    "Generosity": "generosity",
    "Perceptions of corruption": "corruption",
}

# Variantes del nombre de un mismo país entre años -> nombre canónico.
# Regla: el nombre que aparece en más años; en empate (Somaliland), "Somaliland Region".
COUNTRY_NAME_MAP = {
    "Hong Kong S.A.R., China": "Hong Kong",
    "Taiwan Province of China": "Taiwan",
    "Northern Cyprus": "North Cyprus",
    "Trinidad & Tobago": "Trinidad and Tobago",
    "North Macedonia": "Macedonia",
    "Somaliland region": "Somaliland Region",
}

COLUMN_MAP_BY_YEAR = {
    2015: _MAP_2015_2016,
    2016: _MAP_2015_2016,
    2017: _MAP_2017,
    2018: _MAP_2018_2019,
    2019: _MAP_2018_2019,
}
