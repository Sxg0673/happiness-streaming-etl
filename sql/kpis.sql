-- =============================================================================
-- KPI del dashboard · happiness-streaming-etl
--
-- Estas consultas calculan con SQL las mismas cifras que muestra el dashboard de
-- Tableau (dashboards/dashboard.twb). Sirven para dos cosas:
--   1. Comprobar que cada número del dashboard sale de la base de datos.
--   2. Consultar los KPI sin abrir Tableau.
--
-- El dashboard NO lee este archivo: se conecta en vivo a las cuatro tablas y hace
-- los cálculos con sus propios campos calculados. Si las cifras de aquí y las del
-- dashboard no coinciden, hay un error en alguno de los dos.
--
-- Uso:
--   docker compose exec -T postgres psql -U happiness -d happiness < sql/kpis.sql
-- =============================================================================

-- -----------------------------------------------------------------------------
-- KPI 1 · Error promedio de predicción (tarjetas de la barra lateral)
-- -----------------------------------------------------------------------------
-- MAE   error absoluto medio: cuánto se equivoca el modelo en promedio.
-- RMSE  raíz del error cuadrático medio: pesa más los errores grandes.
-- R²    parte de la variación del puntaje real que el modelo explica (1 = perfecto).
-- Sesgo promedio de (predicho - real): positivo si el modelo predice de más.
SELECT
    COUNT(*)                                                  AS predictions,
    ROUND(AVG(absolute_error)::numeric, 3)                    AS mae,
    ROUND(SQRT(AVG(prediction_error ^ 2))::numeric, 3)        AS rmse,
    ROUND((1 - SUM(prediction_error ^ 2)
             / SUM((actual_score - mean_actual) ^ 2))::numeric, 3) AS r2,
    ROUND(AVG(prediction_error)::numeric, 3)                  AS bias
FROM fact_predictions
CROSS JOIN (SELECT AVG(actual_score) AS mean_actual FROM fact_predictions) AS m;

-- -----------------------------------------------------------------------------
-- Salud del streaming: eventos por estado y porcentaje de válidos
-- -----------------------------------------------------------------------------
-- Sale de la tabla cruda, que guarda TODOS los mensajes leídos de Kafka, también
-- los que el contrato rechazó. Por eso no depende de los filtros del dashboard.
SELECT
    status,
    COUNT(*)                                                  AS events,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1)        AS pct
FROM raw_happiness_events
GROUP BY status
ORDER BY events DESC;

-- -----------------------------------------------------------------------------
-- KPI 1 (desglose) · Error promedio por región (barras "Error por región")
-- -----------------------------------------------------------------------------
SELECT
    COALESCE(c.region, 'Sin región')                          AS region,
    COUNT(DISTINCT f.country_id)                              AS countries,
    COUNT(*)                                                  AS predictions,
    ROUND(AVG(f.absolute_error)::numeric, 3)                  AS mae
FROM fact_predictions AS f
JOIN dim_country AS c USING (country_id)
GROUP BY 1
ORDER BY mae DESC;

-- -----------------------------------------------------------------------------
-- KPI 2 · Predicciones por país (mapa y lista de países)
-- -----------------------------------------------------------------------------
-- El mapa pinta cada país según mean_abs_error, en cinco niveles de 0.20.
SELECT
    c.country_name,
    c.region,
    COUNT(*)                                                  AS predictions,
    ROUND(AVG(f.actual_score)::numeric, 2)                    AS mean_actual,
    ROUND(AVG(f.predicted_score)::numeric, 2)                 AS mean_predicted,
    ROUND(AVG(f.absolute_error)::numeric, 2)                  AS mean_abs_error
FROM fact_predictions AS f
JOIN dim_country AS c USING (country_id)
GROUP BY c.country_name, c.region
ORDER BY mean_abs_error DESC, c.country_name;

-- -----------------------------------------------------------------------------
-- KPI 3 · Predicho vs real (gráfico de dispersión: un punto por evento)
-- -----------------------------------------------------------------------------
SELECT
    c.country_name,
    d.year,
    ROUND(f.actual_score::numeric, 3)                         AS actual_score,
    ROUND(f.predicted_score::numeric, 3)                      AS predicted_score,
    ROUND(f.prediction_error::numeric, 3)                     AS prediction_error
FROM fact_predictions AS f
JOIN dim_country AS c USING (country_id)
JOIN dim_date    AS d USING (date_id)
ORDER BY f.absolute_error DESC;

-- -----------------------------------------------------------------------------
-- KPI 4 · Tendencia en el tiempo (barras "Tendencia por año")
-- -----------------------------------------------------------------------------
-- El eje de tiempo es el año del reporte (dim_date), no la hora de llegada del
-- evento: los 240 eventos de la demostración llegan en poco más de un minuto.
SELECT
    d.year,
    COUNT(*)                                                  AS predictions,
    ROUND(AVG(f.absolute_error)::numeric, 3)                  AS mae,
    ROUND(AVG(f.actual_score)::numeric, 3)                    AS mean_actual,
    ROUND(AVG(f.predicted_score)::numeric, 3)                 AS mean_predicted,
    ROUND(AVG(f.prediction_error)::numeric, 3)                AS bias
FROM fact_predictions AS f
JOIN dim_date AS d USING (date_id)
GROUP BY d.year
ORDER BY d.year;
