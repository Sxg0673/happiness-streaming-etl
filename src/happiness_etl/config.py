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

# Rango válido del puntaje de felicidad (escala de Cantril). Lo comparten la
# compuerta de calidad de la Parte A (validate.py) y el contrato del evento
# (contract.py), para que el streaming acepte lo mismo que el ETL dio por limpio.
SCORE_RANGE = (0.0, 10.0)

# Tope de las seis variables explicativas. En los datos reales el máximo es 2.096
# (gdp); 3.0 deja margen y frena valores absurdos (un gdp de 500) que el modelo
# extrapolaría sin avisar. Lo usan validate.py y contract.py.
FEATURE_MAX = 3.0

# División entrenamiento/prueba (sugerencia del PDF: 70/30), estratificada por año.
# La comparten eda.ipynb y model_training.ipynb para que sea la misma partición.
TEST_SIZE = 0.3
RANDOM_STATE = 42

# Modelo (D-031/D-032): regresión lineal con estas variables numéricas + 'year'
# como categoría. 'generosity' se descartó en el EDA (no aporta al error de
# validación cruzada). 'country' y el puntaje real nunca son variables.
MODEL_FEATURES = ("gdp", "family", "health", "freedom", "corruption")

# Artefactos del entrenamiento. El modelo y sus métricas se versionan (son
# pequeños); el 30 % de prueba es lo que transmite el producer (D-034).
MODELS_DIR = ROOT_DIR / "models"
MODEL_FILE = MODELS_DIR / "model.pkl"
METRICS_FILE = MODELS_DIR / "metrics.json"
STREAMING_DIR = ROOT_DIR / "data" / "streaming"
STREAMING_FILE = STREAMING_DIR / "happiness_test.csv"

# Streaming (Parte B). Nombres exigidos por el taller. La dirección es 127.0.0.1
# y no "localhost": el broker se anuncia así (ver docker-compose.yml) y con
# "localhost" el cliente intenta primero IPv6, donde no hay nadie escuchando.
KAFKA_BOOTSTRAP_SERVERS = "127.0.0.1:9092"
KAFKA_TOPIC = "happiness-events"
KAFKA_TOPIC_PARTITIONS = 3
KAFKA_CONSUMER_GROUP = "happiness-ml-team"

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
