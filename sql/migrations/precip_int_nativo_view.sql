-- precip_int_nativo_view
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
SELECT estaciones.unid,
    estaciones.id,
    redes.nombre red,
    estaciones.id_externo,
    estaciones.nombre,
    estaciones.geom,
    series.id AS series_id,
    var.abrev var_abrev,
    series.proc_id proc_id,
    procedimiento.abrev proc_abrev,
    series.unit_id unit_id,
    unidades.abrev unit_abrev,
    to_char(min(observaciones.timestart)::timestamp at time zone 'ART', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') timestart,
    to_char(max(observaciones.timeend)::timestamp at time zone 'ART', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') timeend,
    sum(valores_num.valor) suma,
    round(avg( case 
        when timeend > timestart then valores_num.valor/extract(epoch from (timeend-timestart))*3600*24  
        else valores_num.valor 
        end )::numeric,2) media_diaria,
    json_object(array_agg(timestart::text),   array_agg(valor::text))::text AS valor,
    count(valor) n_obs
   FROM redes
   JOIN estaciones
    ON redes.tabla_id = estaciones.tabla 
   JOIN series
    ON estaciones.unid = series.estacion_id 
   JOIN observaciones
    ON series.id = observaciones.series_id 
   JOIN valores_num
    ON observaciones.id = valores_num.obs_id 
   JOIN var
    ON var.id=series.var_id
   JOIN procedimiento
    ON procedimiento.id=series.proc_id
   JOIN unidades
    ON unidades.id = series.unit_id
  JOIN user_access
      ON user_access.red_id = redes.id
  WHERE 
    series.var_id = 27 
    AND series.proc_id = 1 
    AND series.unit_id = 9 
    AND observaciones.timestart between  '%timeStart%'::timestamp 
    AND '%timeEnd%'::timestamp 
    AND timestart>=current_date-15 
    AND estaciones.habilitar=true 
   GROUP BY redes.nombre,
    estaciones.unid,
    estaciones.id,
    estaciones.tabla,
    estaciones.id_externo,
    estaciones.nombre,
    estaciones.geom,
    series.id,
    var.abrev,
    procedimiento.abrev,
    unidades.abrev
  ORDER BY min(observaciones.timestart),
           max(observaciones.timeend) desc
