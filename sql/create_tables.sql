-- =============================================================================
-- Esquema analítico de predicciones · happiness-streaming-etl
--
-- raw_happiness_events  staging/trazabilidad: cada mensaje de Kafka tal cual llegó,
--                        con su estado de procesamiento. NO es una dimensión.
-- dim_country           un país por fila, con su región (atributo analítico).
-- dim_date              un reporte anual por fila (2015-2019): el año de la
--                        observación histórica, que es el eje de tiempo del análisis.
-- fact_predictions      una predicción por evento válido, enlazada a su evento crudo.
--
-- El script es idempotente (IF NOT EXISTS): se puede ejecutar varias veces.
-- Motor: PostgreSQL 12 o superior (usa columnas generadas).
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Staging: eventos crudos de Kafka
-- -----------------------------------------------------------------------------
-- El consumer inserta aquí ANTES de validar o predecir. raw_payload guarda el
-- mensaje como texto exacto (no JSONB) para conservar incluso un JSON mal formado.
-- La terna (topic, partition, offset) identifica un mensaje de Kafka de forma
-- única: si Kafka reentrega un mensaje tras un reinicio, no se duplica.
CREATE TABLE IF NOT EXISTS raw_happiness_events (
    raw_event_id     BIGSERIAL    PRIMARY KEY,
    kafka_topic      TEXT         NOT NULL,
    kafka_partition  INTEGER      NOT NULL,
    kafka_offset     BIGINT       NOT NULL,
    kafka_key        TEXT,
    kafka_timestamp  TIMESTAMPTZ,
    raw_payload      TEXT         NOT NULL,
    -- RECEIVED: guardado y aún sin procesar (si el consumer cae a mitad de camino,
    -- la fila queda así y se puede reprocesar). Los otros cuatro son estados finales.
    status           TEXT         NOT NULL DEFAULT 'RECEIVED',
    error_message    TEXT,
    received_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),
    processed_at     TIMESTAMPTZ,
    CONSTRAINT uq_raw_kafka_message UNIQUE (kafka_topic, kafka_partition, kafka_offset),
    CONSTRAINT ck_raw_status CHECK (
        status IN ('RECEIVED', 'VALID', 'INVALID_SCHEMA', 'INVALID_VALUES', 'PREDICTION_ERROR')
    ),
    -- Un evento procesado tiene fecha de procesamiento; uno inválido, su motivo.
    CONSTRAINT ck_raw_processed_at CHECK (status = 'RECEIVED' OR processed_at IS NOT NULL),
    CONSTRAINT ck_raw_error_message CHECK (
        status IN ('RECEIVED', 'VALID') OR error_message IS NOT NULL
    )
);

CREATE INDEX IF NOT EXISTS ix_raw_status ON raw_happiness_events (status);

-- -----------------------------------------------------------------------------
-- Dimensión país
-- -----------------------------------------------------------------------------
-- Se precarga con sql/seed_dimensions.sql. Si llega un país nuevo, el consumer
-- lo inserta con region NULL en lugar de rechazar el evento.
CREATE TABLE IF NOT EXISTS dim_country (
    country_id    SERIAL  PRIMARY KEY,
    country_name  TEXT    NOT NULL,
    region        TEXT,
    CONSTRAINT uq_country_name UNIQUE (country_name),
    CONSTRAINT ck_country_name_not_blank CHECK (btrim(country_name) <> '')
);

-- -----------------------------------------------------------------------------
-- Dimensión fecha (granularidad: reporte anual)
-- -----------------------------------------------------------------------------
-- date_id es una clave inteligente = el año (2019). report_date es el 1 de enero
-- de ese año, para que Tableau lo trate como fecha. El momento del streaming
-- (event_time, prediction_timestamp) va como timestamp en la tabla de hechos:
-- todos los eventos se emiten el mismo día, así que no tendría sentido agruparlos
-- en una dimensión diaria.
CREATE TABLE IF NOT EXISTS dim_date (
    date_id       SMALLINT  PRIMARY KEY,
    year          SMALLINT  NOT NULL,
    report_date   DATE      NOT NULL,
    report_name   TEXT      NOT NULL,
    CONSTRAINT uq_date_year UNIQUE (year),
    CONSTRAINT ck_date_id_is_year CHECK (date_id = year),
    CONSTRAINT ck_date_report_date CHECK (report_date = make_date(year, 1, 1))
);

-- -----------------------------------------------------------------------------
-- Hechos: predicciones
-- -----------------------------------------------------------------------------
-- Grano: una fila por evento válido con predicción (raw_event_id es único).
-- prediction_error = predicho - real (con signo: positivo = sobreestima) y
-- absolute_error = |error|. Son columnas generadas: las calcula la base de datos,
-- así nunca quedan inconsistentes con los puntajes guardados.
CREATE TABLE IF NOT EXISTS fact_predictions (
    prediction_id         BIGSERIAL         PRIMARY KEY,
    raw_event_id          BIGINT            NOT NULL,
    event_id              TEXT              NOT NULL,
    country_id            INTEGER           NOT NULL,
    date_id               SMALLINT          NOT NULL,
    actual_score          DOUBLE PRECISION  NOT NULL,
    predicted_score       DOUBLE PRECISION  NOT NULL,
    prediction_error      DOUBLE PRECISION  GENERATED ALWAYS AS (predicted_score - actual_score) STORED,
    absolute_error        DOUBLE PRECISION  GENERATED ALWAYS AS (abs(predicted_score - actual_score)) STORED,
    event_time            TIMESTAMPTZ       NOT NULL,
    prediction_timestamp  TIMESTAMPTZ       NOT NULL DEFAULT now(),
    model_version         TEXT              NOT NULL,
    CONSTRAINT uq_fact_raw_event UNIQUE (raw_event_id),
    CONSTRAINT fk_fact_raw_event FOREIGN KEY (raw_event_id) REFERENCES raw_happiness_events (raw_event_id),
    CONSTRAINT fk_fact_country FOREIGN KEY (country_id) REFERENCES dim_country (country_id),
    CONSTRAINT fk_fact_date FOREIGN KEY (date_id) REFERENCES dim_date (date_id),
    CONSTRAINT ck_fact_event_id_not_blank CHECK (btrim(event_id) <> ''),
    CONSTRAINT ck_fact_actual_score CHECK (actual_score BETWEEN 0 AND 10)
);

CREATE INDEX IF NOT EXISTS ix_fact_country ON fact_predictions (country_id);
CREATE INDEX IF NOT EXISTS ix_fact_date ON fact_predictions (date_id);
CREATE INDEX IF NOT EXISTS ix_fact_event_id ON fact_predictions (event_id);
