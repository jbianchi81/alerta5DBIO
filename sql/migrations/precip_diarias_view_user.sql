WITH user_access AS (
  SELECT user_id,
    red_id
  FROM user_red_access
  WHERE user_red_access.user_id=COALESCE(
        token_user_id(
          '%token%', 
          current_setting('app.token_secret')
        ),
        10
      )
)
SELECT estaciones.unid as sitecode,
    estaciones.geom,
    redes.nombre red,
    estaciones.nombre,
    series.id AS series_id,
    series.var_id as var_id,
    series.proc_id as proc_id,
    series.unit_id as unit_id,
     to_char(min(observaciones.timestart)::timestamp at time zone 'ART', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') timestart,
    to_char(max(observaciones.timeend)::timestamp at time zone 'ART', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') timeend,
    sum(valores_num.valor) suma,
    count(valores_num.valor) n_obs,
    round(avg(valor)::numeric,2) media_diaria,
    min(valores_num.valor) as minimo,
    max(valores_num.valor) as maximo,
    json_object(array_agg(timestart::text),   array_agg(round(valor::numeric,2)::text))::text AS valor,
    estaciones.id_externo,
    estaciones.id,
    redes.public,
    redes.public_his_plata,
    estaciones.tipo,
    redes.id redid
   FROM redes  
    JOIN estaciones 
      ON redes.tabla_id = estaciones.tabla 
    JOIN series
      ON estaciones.unid = series.estacion_id  
    JOIN observaciones
      ON series.id = observaciones.series_id 
    JOIN valores_num
      ON observaciones.id = valores_num.obs_id
    JOIN user_access
      ON user_access.red_id = redes.id
  WHERE 
    series.var_id = 1
    AND series.proc_id = 1 
    AND series.unit_id = 22 
    AND observaciones.timestart::date >= case when '%timeStart%'='1800-01-01' 
                                        then current_date-'7 days'::interval
                                        else '%timeStart%'::date end
	AND observaciones.timeend::date <= case when '%timeEnd%' = '1800-01-01' 
	                                  then current_date
	                                  else '%timeEnd%'::date end
    AND timestart>=current_date-'3 months'::interval
    AND estaciones.habilitar=true 
    AND redes.id= CASE WHEN %redid%!=0 THEN %redid% ELSE redes.id END AND estaciones.tipo= CASE WHEN '%tipo%' != 'X' THEN '%tipo%' else estaciones.tipo END
    GROUP BY redes.nombre,
		estaciones.unid,
		estaciones.id,
		estaciones.tabla,
		estaciones.id_externo,
		estaciones.nombre,
		estaciones.geom,
		series.id,
		redes.public,
		redes.public_his_plata,
		estaciones.tipo,
		redes.id
  ORDER BY max(observaciones.timeend) desc
