# Happiness Streaming ETL

Pipeline ETL en streaming con Apache Kafka y un modelo de regresión preentrenado que predice en tiempo real el puntaje de felicidad (World Happiness, datos 2015-2019).

Curso: ETL (G01) - Workshop-3, Universidad Autónoma de Occidente.

## Descripción del proyecto

El proyecto tiene dos partes:

- **Parte A (offline):** perfilar los cinco archivos anuales de World Happiness, limpiarlos y armonizarlos en un solo dataset, y entrenar un modelo de regresión guardado como `model.pkl`.
- **Parte B (streaming):** un producer de Kafka publica cada registro como un evento; un consumer lo valida, predice el puntaje y guarda los eventos crudos y las predicciones en una base de datos que alimenta el dashboard.

## Arquitectura

_Pendiente (diagrama completo al final del proyecto)._

La Parte A es un paquete de Python (`src/happiness_etl/`) que ejecuta `main.py`:

```
data/raw/*.csv -> extract -> clean -> transform -> validate (compuerta de calidad) -> load -> data/processed/happiness_unified.csv
```

| Módulo | Responsabilidad |
|---|---|
| `config.py` | Rutas, años, nombres canónicos y mapa de columnas de cada año (única fuente de verdad) |
| `extract.py` | Leer los CSV originales tal como vienen |
| `clean.py` | Por año: nombres canónicos de columna, columna `year`, texto del país, tipos numéricos y duplicados exactos |
| `transform.py` | Entre años: unir, unificar nombres de país, interpolar valores faltantes y ordenar |
| `validate.py` | Compuerta de calidad antes de cargar y reconciliación después de cargar |
| `load.py` | Escritura atómica del CSV y lectura con tipos del dataset unificado |

Los pasos intermedios se pasan en memoria; solo el dataset final ya validado se escribe en disco, porque los notebooks lo leen desde procesos separados. El archivo no se versiona: se regenera con `main.py`.

`notebooks/model_training.ipynb` divide ese dataset en entrenamiento (70 %) y prueba (30 %), guarda el modelo en `models/model.pkl` y escribe la parte de prueba en `data/streaming/happiness_test.csv`. Ese archivo, versionado, es lo que transmite el producer de Kafka, para que las métricas del dashboard se calculen sobre datos que el modelo no vio.

## Decisiones de limpieza de datos

El perfilado de calidad está en [`notebooks/profiling.ipynb`](notebooks/profiling.ipynb) (Paso 1 del taller). Decisiones principales:

| Hallazgo | Decisión |
|---|---|
| Un mismo concepto tiene hasta tres nombres de columna según el año | Se asigna a cada año los nombres canónicos (`config.COLUMN_MAP_BY_YEAR`) |
| Los archivos no traen columna de año | Se agrega `year` a partir del archivo de origen |
| Columnas que no se usan: rankings, `Region`, error estándar, intervalos de confianza, *whiskers* y `Dystopia Residual` | No pasan al dataset unificado. El ranking y el residual dependen del puntaje, así que usarlos sería fuga del target. `Region` (solo 2015-2016) se aprovecha como atributo de `dim_country` en la base de datos |
| Un único valor faltante: `corruption` de United Arab Emirates en 2018 | Interpolación lineal dentro del país (promedio de 2017 y 2019, 0.253). Solo entre años observados, sin extrapolar; queda registrado como aviso en el log |
| Seis países se escriben distinto según el año (p. ej. `Trinidad & Tobago`) | Se usa el nombre que aparece en más años (`config.COUNTRY_NAME_MAP`): 170 nombres pasan a ser 164 países |
| Un cero por variable y año, y 102 outliers según IQR | Se conservan: los ceros son el mínimo de cada escala y los outliers son valores plausibles, no errores |
| `family` y `health` cambian de escala entre años | No se modifica en la limpieza; se estudia en el EDA del modelo |

La compuerta de calidad (`validate.py`) detiene el pipeline antes de escribir si falla alguna regla y reporta todas las fallas juntas: esquema exacto, sin nulos, país no vacío, año entre 2015 y 2019, variables no negativas, puntaje entre 0 y 10 y par (país, año) único. Resultado: 782 filas, 164 países y ningún valor faltante.

## Decisiones de ingeniería de características

_Pendiente._

## Pipeline de Kafka y contrato del evento

_Pendiente._

## Esquema de la base de datos

PostgreSQL con un esquema en estrella: una tabla de hechos con las predicciones, dos dimensiones y una tabla de staging para la trazabilidad.

| Archivo | Contenido |
|---|---|
| `sql/create_tables.sql` | Tablas, llaves, restricciones e índices (idempotente). |
| `sql/seed_dimensions.sql` | Carga inicial de `dim_country` (164 países con su región) y `dim_date` (2015-2019). |
| `docs/diagrams/dimensional_model.dbml` | Diagrama del modelo para [dbdiagram.io](https://dbdiagram.io). |

Diagrama generado en dbdiagram.io a partir de `docs/diagrams/dimensional_model.dbml`:

![Modelo dimensional de predicciones](docs/img/dimensional_model.png)

Versión equivalente en Mermaid (GitHub la dibuja directamente):

```mermaid
erDiagram
    raw_happiness_events ||--o| fact_predictions : "genera (si es válido)"
    dim_country ||--o{ fact_predictions : ""
    dim_date ||--o{ fact_predictions : ""

    raw_happiness_events {
        bigserial raw_event_id PK
        text kafka_topic
        int kafka_partition
        bigint kafka_offset
        text raw_payload
        text status
        text error_message
        timestamptz received_at
        timestamptz processed_at
    }
    fact_predictions {
        bigserial prediction_id PK
        bigint raw_event_id FK
        text event_id
        int country_id FK
        smallint date_id FK
        double actual_score
        double predicted_score
        double prediction_error
        double absolute_error
        timestamptz event_time
        timestamptz prediction_timestamp
        text model_version
    }
    dim_country {
        serial country_id PK
        text country_name
        text region
    }
    dim_date {
        smallint date_id PK
        smallint year
        date report_date
        text report_name
    }
```

| Tabla | Grano | Decisiones |
|---|---|---|
| `raw_happiness_events` | Un mensaje de Kafka | Staging, no dimensión. El consumer inserta **antes** de validar; `raw_payload` es el texto exacto (conserva incluso un JSON mal formado). `status`: `RECEIVED` (guardado, aún sin procesar) y los cuatro del taller: `VALID`, `INVALID_SCHEMA`, `INVALID_VALUES`, `PREDICTION_ERROR`. Un estado inválido exige `error_message`. `(topic, partition, offset)` es único, así una reentrega de Kafka no duplica filas. |
| `fact_predictions` | Una predicción por evento válido | `raw_event_id` es único y enlaza cada predicción con el mensaje que la originó. `prediction_error = predicted_score − actual_score` (con signo: positivo = sobreestima) y `absolute_error` son **columnas generadas**: las calcula PostgreSQL, por lo que nunca quedan inconsistentes. `model_version` identifica el modelo que predijo. |
| `dim_country` | Un país | `region` viene de la columna Region de 2015-2016; Gambia (solo aparece en 2019) se asignó a mano a Sub-Saharan Africa. Un país nuevo se inserta con región vacía en lugar de rechazar el evento. |
| `dim_date` | Un reporte anual | `date_id` = año. Es el eje temporal del análisis (tendencia del error por año). El momento del streaming queda en `event_time` y `prediction_timestamp`: todos los eventos se emiten el mismo día, así que una dimensión diaria no aportaría. |

## Dashboard

_Pendiente._

## Instrucciones de ejecución

_Parcial: la Parte A y la infraestructura están listas; el producer y el consumer se agregan después._

### Parte A: dataset y modelo

```bash
python3.12 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

python -m happiness_etl.main   # genera data/processed/happiness_unified.csv
python -m pytest -q            # ejecuta las pruebas
```

`main.py` termina con código 1 y lista las reglas incumplidas si la compuerta de calidad detiene el pipeline.

### Infraestructura: Kafka y PostgreSQL

Requiere Docker Desktop en marcha. El producer, el consumer y los notebooks corren en el entorno virtual y se conectan por `127.0.0.1`.

```bash
docker compose up -d --wait   # levanta Kafka y PostgreSQL, crea el topic y carga el esquema
docker compose ps             # estado de los servicios
docker compose down           # detiene todo; los datos se conservan en volúmenes
```

| Servicio | Dirección | Detalles |
|---|---|---|
| Kafka 3.9.1 (modo KRaft, un solo broker) | `127.0.0.1:9092` | Topic `happiness-events` con 3 particiones y replicación 1. La creación automática de topics está desactivada: un nombre mal escrito falla en lugar de crear otro topic. |
| PostgreSQL 16.15 | `127.0.0.1:5432` | Base, usuario y contraseña `happiness` por defecto (se cambian copiando `.env.example` a `.env`). El esquema y los datos de referencia se cargan solo la primera vez. |

Los puertos solo escuchan en el propio equipo. Kafka y PostgreSQL guardan sus datos en volúmenes, así que `docker compose down` no borra eventos ni predicciones. **No usar `docker compose down -v`**: elimina los volúmenes.

Para volver a aplicar el esquema sobre una base ya creada (los scripts son idempotentes; ajustar usuario y base si se cambiaron en `.env`):

```bash
docker compose exec -T postgres psql -U happiness -d happiness -v ON_ERROR_STOP=1 < sql/create_tables.sql
docker compose exec -T postgres psql -U happiness -d happiness -v ON_ERROR_STOP=1 < sql/seed_dimensions.sql
```

Las dependencias de Python para conectarse son `confluent-kafka` (Kafka) y `psycopg` (PostgreSQL), ya incluidas en `requirements.txt`.
