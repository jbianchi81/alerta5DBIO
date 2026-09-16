-- view series_areal_prono_last

CREATE OR REPLACE VIEW series_areal_prono_last AS
WITH corridas_max_date AS (
         SELECT corridas.cal_id,
            max(corridas.date) AS date
           FROM corridas
          GROUP BY corridas.cal_id
        ), corridas_last AS (
         SELECT corridas.cal_id,
            corridas.date AS fecha_emision,
            corridas.id AS cor_id,
            calibrados.nombre,
            calibrados.modelo,
            calibrados.model_id,
            calibrados.public,
            calibrados.grupo_id
           FROM corridas,
            corridas_max_date,
            calibrados
          WHERE corridas.date = corridas_max_date.date AND corridas.cal_id = corridas_max_date.cal_id AND corridas.cal_id = calibrados.id
          ORDER BY corridas.cal_id
        ), series_prono_last AS (
         SELECT corridas_last.cal_id,
            corridas_last.fecha_emision,
            corridas_last.cor_id,
            corridas_last.nombre,
            corridas_last.modelo,
            corridas_last.model_id,
            corridas_last.public,
            corridas_last.grupo_id,
            pronosticos_areal.series_id,
            min(pronosticos_areal.timestart) AS timestart,
            max(pronosticos_areal.timeend) AS timeend,
            count(pronosticos_areal.timestart) AS count
           FROM corridas_last,
            pronosticos_areal
          WHERE corridas_last.cor_id = pronosticos_areal.cor_id
          GROUP BY corridas_last.cal_id, corridas_last.fecha_emision, corridas_last.cor_id, corridas_last.grupo_id, corridas_last.nombre, corridas_last.modelo, corridas_last.model_id, pronosticos_areal.series_id, corridas_last.public
        )
 SELECT series_prono_last.cal_id,
    series_prono_last.fecha_emision,
    series_prono_last.cor_id,
    series_prono_last.nombre,
    series_prono_last.modelo,
    series_prono_last.model_id,
    series_prono_last.public,
    series_prono_last.grupo_id AS cal_grupo_id,
    series_prono_last.series_id,
    series_prono_last.timestart,
    series_prono_last.timeend,
    series_prono_last.count,
    areas_pluvio.nombre AS estacion_nombre,
    areas_pluvio.unid AS estacion_id,
    var.nombre AS var_nombre,
    var.id AS var_id,
    fuentes.nombre AS fuente_nombre,
    fuentes.id AS fuentes_id
   FROM series_prono_last,
    series_areal,
    areas_pluvio,
    var,
    fuentes
  WHERE series_prono_last.series_id = series_areal.id 
  AND series_areal.area_id = areas_pluvio.unid 
  AND series_areal.var_id = var.id
  AND series_areal.fuentes_id = fuentes.id
  ORDER BY series_prono_last.cal_id, var.id, areas_pluvio.unid;
