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
| `contract.py` | Parte B: contrato del evento (armar y validar eventos) |
| `db.py` | Parte B: datos de conexión a PostgreSQL (entorno, `.env` o valores por defecto) |

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

La compuerta de calidad (`validate.py`) detiene el pipeline antes de escribir si falla alguna regla y reporta todas las fallas juntas: esquema exacto, sin nulos, país no vacío, año entre 2015 y 2019, variables entre 0 y 3, puntaje entre 0 y 10 y par (país, año) único. Resultado: 782 filas, 164 países y ningún valor faltante.

## Decisiones de ingeniería de características

_Pendiente._

## Pipeline de Kafka y contrato del evento

_Parcial: el contrato y el producer están listos; el consumer se agrega después._

```
data/streaming/happiness_test.csv -> producer -> topic happiness-events (3 particiones) -> consumer -> PostgreSQL
```

### Contrato del evento

Cada mensaje es un objeto JSON en UTF-8 con estos once campos. La llave del mensaje de Kafka es el país, así que todos los eventos de un país caen en la misma partición y conservan su orden.

```json
{
  "event_id": "3de398aa-28fb-401a-8bb3-98991bfc49b7",
  "country": "Namibia",
  "year": 2016,
  "gdp": 0.93287,
  "family": 0.70362,
  "health": 0.34745,
  "freedom": 0.48614,
  "generosity": 0.07795,
  "corruption": 0.10398,
  "actual_happiness_score": 4.574,
  "event_time": "2026-11-11T00:55:07.005Z"
}
```

| Campo | Tipo | Regla |
|---|---|---|
| `event_id` | texto | No vacío. Es un UUID al azar: identifica el evento de punta a punta (registro del producer, tabla cruda y tabla de hechos). |
| `country` | texto | No vacío. También es la llave del mensaje. |
| `year` | entero | Entre 2015 y 2019. No se aceptan `"2019"` ni `2019.5`. |
| `gdp`, `family`, `health`, `freedom`, `generosity`, `corruption` | número | Finito, entre 0 y 3. El máximo real en los datos es 2.096; el tope de 3 (`FEATURE_MAX` en `config.py`) frena valores absurdos que el modelo extrapolaría sin avisar. |
| `actual_happiness_score` | número | Entre 0 y 10. Es el puntaje real: viaja solo para medir el error, **nunca es una variable del modelo**. |
| `event_time` | texto | Fecha y hora ISO-8601: el momento en que el producer emitió el evento, en UTC. Si llega sin zona horaria (como en el ejemplo del taller) se interpreta como UTC. |

El contrato vive en un solo módulo, [`src/happiness_etl/contract.py`](src/happiness_etl/contract.py): el producer arma los eventos con `build_event` y el consumer valida cada mensaje con `validate_event`. Los rangos son los mismos de la compuerta de calidad de la Parte A (salen de `config.py`), así que el streaming acepta exactamente lo que el ETL dio por limpio.

`validate_event` nunca lanza una excepción por el contenido de un mensaje: devuelve un estado y **todos** los motivos encontrados, que se guardan en `error_message`.

| Estado | Cuándo | Ejemplos |
|---|---|---|
| `VALID` | Cumple el contrato | |
| `INVALID_SCHEMA` | El mensaje no se puede interpretar | No es JSON o no es un objeto, falta un campo, texto en un campo numérico, `year` decimal, `event_time` que no es una fecha |
| `INVALID_VALUES` | La forma es correcta pero un valor está fuera de rango | Año 2020, variable negativa o mayor que 3, puntaje 11, `NaN` |
| `PREDICTION_ERROR` | El evento era válido pero el modelo falló | Lo asigna el consumer |

Si hay problemas de esquema no se evalúan los rangos. Los campos que sobran se ignoran: el mensaje original queda completo en la tabla cruda.

### Producer

[`kafka/producer.py`](kafka/producer.py) lee el conjunto de prueba (235 registros que el modelo no vio), arma un evento por registro y los publica uno por uno.

| Opción | Por defecto | Qué hace |
|---|---|---|
| `--delay` | `0.5` | Segundos de pausa entre eventos (simula la llegada en el tiempo; los 235 tardan unos 2 minutos). |
| `--limit N` | todos | Envía solo los primeros N registros. |
| `--with-invalid` | no | Intercala 5 eventos dañados a propósito para mostrar la validación. |
| `--seed` | `42` | Semilla del orden de envío. |

Decisiones:

- **Orden mezclado y reproducible.** Los registros se mezclan con una semilla fija para que lleguen países y años variados, como en un flujo real, y siempre en el mismo orden.
- **Entrega confirmada.** El producer es idempotente (`enable.idempotence`): el broker confirma cada evento y descarta los duplicados de un reintento. Se espera la confirmación de cada evento antes de enviar el siguiente y se registra en qué partición y offset quedó.
- **Fallas claras.** Si Kafka no responde, el topic no existe o un evento no se puede entregar, el envío se detiene con un mensaje y código de salida 1 (130 si se interrumpe con Ctrl+C).
- **Eventos dañados para la demostración.** Los datos de prueba están limpios, así que sin `--with-invalid` nunca llegaría un evento inválido. Con esa opción el resultado es siempre el mismo: 240 mensajes, de los cuales 235 son `VALID`, 3 `INVALID_SCHEMA` (falta `health`, texto en `gdp`, JSON cortado) y 2 `INVALID_VALUES` (año 2020, `gdp` negativo).

### Reinicio de la demostración

Kafka y PostgreSQL conservan sus datos entre corridas, y cada corrida del producer genera `event_id` nuevos: si se ejecuta dos veces, las predicciones se duplican. [`scripts/reset_demo.py`](scripts/reset_demo.py) deja todo en cero en un solo paso: vacía `raw_happiness_events` y `fact_predictions` (las dimensiones no se tocan), borra el consumer group `happiness-ml-team` y recrea el topic. Las tres cosas van juntas porque, si se recreara el topic sin vaciar las tablas, los offsets volverían a empezar en 0 y chocarían con los ya guardados. Se niega a correr si el consumer sigue activo y no borra volúmenes de Docker.

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

_Parcial: la Parte A, la infraestructura y el producer están listos; el consumer se agrega después._

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

Las dependencias de Python para conectarse son `confluent-kafka` (Kafka) y `psycopg` (PostgreSQL), ya incluidas en `requirements.txt`. Los scripts leen los datos de conexión a PostgreSQL de las variables de entorno, del archivo `.env` o de los valores por defecto, en ese orden.

### Parte B: streaming

Con la infraestructura arriba y el entorno virtual activo:

```bash
python scripts/reset_demo.py                      # deja la demo en cero (pide confirmación; --yes para no preguntar)
python kafka/producer.py --with-invalid           # 240 eventos, uno cada 0.5 s
python kafka/producer.py --delay 0 --limit 10     # prueba rápida
```

El producer registra una línea por evento con el país, el año, la partición, el offset y el `event_id`.
