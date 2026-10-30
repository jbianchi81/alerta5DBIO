-- series_areal_prono
WITH corridas_max_date AS (
  SELECT
    cal_id,
    max(date) AS date
  FROM series_areal_prono_date_range 
  JOIN corridas
    ON cor_id=corridas.id 
  WHERE cal_id=%calId%
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
   FROM corridas
   JOIN corridas_max_date
	ON corridas.date = corridas_max_date.date AND corridas.cal_id = corridas_max_date.cal_id
   JOIN calibrados
	ON corridas.cal_id = calibrados.id
  ORDER BY corridas.cal_id
),
series_prono_last AS (
  SELECT 
	corridas_last.cal_id,
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
	count(pronosticos_areal.timestart) AS count,
	round(sum(pronosticos_areal.valor)::numeric,2) suma,
    round(min(pronosticos_areal.valor)::numeric,2) minimo,
    round(max(pronosticos_areal.valor)::numeric,2) maximo,
    count(pronosticos_areal.valor) n_obs,
    round(avg(pronosticos_areal.valor)::numeric,2) media,
    sum(pronosticos_areal.timeend - pronosticos_areal.timestart)::text as t_sum,
    round(%function%(pronosticos_areal.valor)::numeric,2)::numeric as valor_agg,
    '%function%'::text as agg_function,
    json_object(array_agg(pronosticos_areal.timestart::text order by pronosticos_areal.timestart), array_agg(round(pronosticos_areal.valor::numeric, 2)::text order by pronosticos_areal.timestart))::text AS valor
   FROM corridas_last
   JOIN pronosticos_areal
	ON corridas_last.cor_id=pronosticos_areal.cor_id
  WHERE  
       pronosticos_areal.qualifier='%qualifier%'
  and
       pronosticos_areal.timestart>=case when '%timeStart%'='1800-01-01' then current_timestamp-'7 days'::interval else '%timeStart%'::timestamp end
  and 
       pronosticos_areal.timeend<=case when '%timeEnd%'='1800-01-01' then current_timestamp+'7 days'::interval else '%timeEnd%'::timestamp end 

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
    series_prono_last.suma,
    series_prono_last.minimo,
    series_prono_last.maximo,
    series_prono_last.n_obs,
    series_prono_last.media,
    series_prono_last.t_sum,
    series_prono_last.valor_agg,
    series_prono_last.agg_function,
    series_prono_last.valor,
    areas_pluvio.nombre AS estacion_nombre,
    areas_pluvio.unid AS estacion_id,
    areas_pluvio.geom AS geom,
    var.nombre AS var_nombre,
    var.id AS var_id,
    fuentes.nombre AS fuente_nombre,
    fuentes.id AS fuentes_id
   FROM series_prono_last
   JOIN
    series_areal
    ON series_prono_last.series_id = series_areal.id
    JOIN 
	  areas_pluvio
		ON series_areal.area_id = areas_pluvio.unid
	JOIN
	  var
		ON series_areal.var_id = var.id
    JOIN
	  fuentes
		ON series_areal.fuentes_id = fuentes.id
	WHERE var.id = %varId%
	AND 
       areas_pluvio.activar=true 
	AND
       areas_pluvio.mostrar=true 
  ORDER BY series_prono_last.cal_id, var.id, areas_pluvio.unid
