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
   FROM redes,
    estaciones,
    series,
    observaciones,
    valores_num
  WHERE redes.tabla_id = estaciones.tabla 
    AND series.var_id = 1
    AND series.proc_id = 1 
    AND series.unit_id = 22 
    AND estaciones.unid = series.estacion_id 
    AND series.id = observaciones.series_id 
    AND observaciones.id = valores_num.obs_id 
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