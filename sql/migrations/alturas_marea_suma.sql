SELECT estaciones.unid estacion_id,
    estaciones.id,
    redes.nombre red,
    estaciones.id_externo,
    estaciones.nombre,
    estaciones.geom,
    series.id AS series_id,
    series.var_id var_id,
    series.proc_id proc_id,
    series.unit_id unit_id,
    to_char(min(observaciones.timestart)::timestamp at time zone 'ART', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') timestart,
    to_char(max(observaciones.timeend)::timestamp at time zone 'ART', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') timeend,
    '%function%' as "function",
    round(%function%(valores_num.valor)::numeric,2) "aggValue",
    json_object(array_agg(timestart::text),array_agg(round(valores_num.valor::numeric,3)::text))::text valor,
    count(valor) n_obs
   FROM redes,
    estaciones,
    series,
    observaciones,
    valores_num
  WHERE redes.tabla_id = estaciones.tabla 
    AND estaciones.tabla = 'puntos_FDelta'
    AND series.var_id = 2 
    AND series.proc_id = 4 
    AND series.unit_id = 11 
    AND estaciones.unid = series.estacion_id 
    AND series.id = observaciones.series_id 
    AND observaciones.id = valores_num.obs_id 
    AND observaciones.timestart >= case when '%timeStart%'='1800-01-01' 
                                        then current_date
                                        else '%timeStart%'::timestamp end
	AND observaciones.timeend <= case when '%timeEnd%' = '1800-01-01' 
	                                  then current_date+'1 day'::interval
	                                  else '%timeEnd%'::timestamp end
    GROUP BY redes.nombre,
		estaciones.unid,
		estaciones.id,
		estaciones.tabla,
		estaciones.id_externo,
		estaciones.nombre,
		estaciones.geom,
		series.id
  ORDER BY min(observaciones.timestart),max(observaciones.timeend) desc
