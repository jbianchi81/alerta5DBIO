WITH last_cor AS (
SELECT 
    corridas.cal_id,
    series_areal_prono_date_range.series_id,
    max(corridas.date) AS date
FROM series_areal_prono_date_range
JOIN corridas ON corridas.id=series_areal_prono_date_range.cor_id
GROUP BY 
    corridas.cal_id, 
    series_areal_prono_date_range.series_id
)
SELECT 
    last_cor.cal_id,
    last_cor.series_id,
    last_cor.date AS fecha_emision,
    corridas.id AS cor_id,
    calibrados.nombre,
    calibrados.modelo,
    calibrados.model_id,
    calibrados.public,
    calibrados.grupo_id AS cal_grupo_id,
    series_areal_prono_date_range.begin_date AS timestart,
    series_areal_prono_date_range.end_date AS timeend,
    series_areal_prono_date_range.count,
    areas_pluvio.nombre AS estacion_nombre,
    areas_pluvio.unid AS estacion_id,
    var.nombre AS var_nombre,
    var.id AS var_id
FROM last_cor
JOIN corridas ON last_cor.date = corridas.date 
JOIN calibrados ON last_cor.cal_id=calibrados.id
JOIN series_areal_prono_date_range ON (
    corridas.id = series_areal_prono_date_range.cor_id 
    AND series_areal_prono_date_range.series_id = last_cor.series_id
)
JOIN series_areal ON series_areal.id = last_cor.series_id
JOIN areas_pluvio ON areas_pluvio.unid = series_areal.area_id
JOIN var ON var.id = series_areal.var_id
ORDER BY last_cor.series_id, last_cor.date
;