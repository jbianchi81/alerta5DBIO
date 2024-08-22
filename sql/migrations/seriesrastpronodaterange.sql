BEGIN;

CREATE OR REPLACE VIEW series_rast_prono_date_range AS
    SELECT
        series_id,
        cor_id,
        min(begin_date) begin_date,
        max(end_date) end_date,
        sum(count) count,
        json_agg(DISTINCT qualifier) qualifiers
    FROM series_rast_prono_date_range_by_qualifier
    GROUP BY series_id, cor_id
    ORDER BY series_id, cor_id;

-- DROP VIEW series_prono_date_range;

CREATE OR REPLACE VIEW series_prono_date_range AS
SELECT series.id AS series_id,
    'series' AS series_table,
    series.estacion_id,
    estaciones.tabla,
    series.var_id,
    corridas.id AS cor_id,
    series_puntual_prono_date_range.begin_date,
    series_puntual_prono_date_range.end_date,
    series_puntual_prono_date_range.count::int,  
    series_puntual_prono_date_range.qualifiers
   FROM corridas 
   JOIN series_puntual_prono_date_range ON series_puntual_prono_date_range.cor_id = corridas.id
   JOIN series ON series.id=series_puntual_prono_date_range.series_id
   JOIN estaciones ON estaciones.unid = series.estacion_id

UNION ALL

SELECT series_areal.id AS series_id,
    'series_areal' AS series_table,
    series_areal.area_id AS estacion_id,
    estaciones.tabla,
    series_areal.var_id,
    corridas.id AS cor_id,
    series_areal_prono_date_range.begin_date,
    series_areal_prono_date_range.end_date,
    series_areal_prono_date_range.count::int,
    series_areal_prono_date_range.qualifiers
   FROM corridas 
   JOIN series_areal_prono_date_range ON series_areal_prono_date_range.cor_id = corridas.id
   JOIN series_areal ON series_areal.id=series_areal_prono_date_range.series_id
   JOIN areas_pluvio ON series_areal.area_id = areas_pluvio.unid
   LEFT JOIN estaciones ON areas_pluvio.exutorio_id = estaciones.unid

UNION ALL

SELECT series_rast.id AS series_id,
    'series_rast' AS series_table,
    series_rast.escena_id AS estacion_id,
    NULL AS tabla,
    series_rast.var_id,
    corridas.id AS cor_id,
    series_rast_prono_date_range.begin_date,
    series_rast_prono_date_range.end_date,
    series_rast_prono_date_range.count::int,
    series_rast_prono_date_range.qualifiers
   FROM corridas 
   JOIN series_rast_prono_date_range ON series_rast_prono_date_range.cor_id = corridas.id
   JOIN series_rast ON series_rast.id=series_rast_prono_date_range.series_id
   JOIN escenas ON series_rast.escena_id = escenas.id
;



COMMIT;