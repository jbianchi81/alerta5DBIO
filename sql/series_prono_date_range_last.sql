BEGIN;
DROP VIEW IF EXISTS series_prono_date_range_last;
CREATE OR REPLACE VIEW series_prono_date_range_last AS
WITH last_forecast_date AS (
    select 
        cal_id,
        max(date) AS forecast_date
    from corridas
    group by cal_id 
),
last_corridas AS (
    select 
        corridas.id AS cor_id,
        corridas.cal_id,
        corridas.date AS forecast_date,
        calibrados.public,
        calibrados.grupo_id AS cal_grupo_id
    FROM corridas
    JOIN calibrados ON corridas.cal_id=calibrados.id
    JOIN last_forecast_date ON (
        corridas.cal_id=last_forecast_date.cal_id 
        AND corridas.date=last_forecast_date.forecast_date
    )
)
SELECT 
    series_prono_date_range.*,
    last_corridas.cal_id,
    last_corridas.forecast_date,
    last_corridas.public,
    last_corridas.cal_grupo_id    
FROM series_prono_date_range
JOIN last_corridas
ON series_prono_date_range.cor_id=last_corridas.cor_id
ORDER BY 
    series_prono_date_range.series_table,
    series_prono_date_range.series_id,
    series_prono_date_range.cor_id;

GRANT SELECT ON series_prono_date_range_last TO actualiza;
COMMIT;