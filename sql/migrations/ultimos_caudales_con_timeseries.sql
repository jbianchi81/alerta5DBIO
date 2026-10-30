-- ultimos_caudales_con_timeseries
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
),
  caudales_last AS (
         SELECT series.estacion_id AS unid,
            observaciones.timestart,
            series.id AS series_id,
            valores_num.valor
           FROM estaciones_view
           JOIN series
            ON estaciones_view.sitecode=series.estacion_id
           JOIN observaciones
            ON series.id = observaciones.series_id
           JOIN valores_num
            ON observaciones.id = valores_num.obs_id
           JOIN user_access
              ON user_access.red_id = estaciones_view.redid
          WHERE  
            series.var_id = 4 
          AND (series.proc_id = ANY (ARRAY[1, 2])) 
          AND series.unit_id = 10 
          AND observaciones.timestart >= case when '%timeStart%'='1800-01-01' 
                                        then current_timestamp-'7 days'::interval
                                        else '%timeStart%'::timestamp end
       	  AND observaciones.timeend <= case when '%timeEnd%' = '1800-01-01' 
	                                  then current_timestamp
	                                  else '%timeEnd%'::timestamp end
	        AND estaciones_view.redid= CASE WHEN %redid%!=0 THEN %redid% ELSE estaciones_view.redid END 
	        AND estaciones_view.tipo= CASE WHEN '%tipo%' != 'X' THEN '%tipo%' else estaciones_view.tipo END
        ), ult AS (
         SELECT caudales_last.unid,
            max(caudales_last.timestart) AS date,
            caudales_last.series_id,
            array_agg(array[to_char(timestart::timestamp at time zone 'ART','YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),round(valor::numeric,2)::text] order by timestart) AS timeseries,
            to_char(min(timestart::timestamp at time zone 'ART'),'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') timestart,
            to_char(max(timestart::timestamp at time zone 'ART'),'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') timeend
           FROM caudales_last
          WHERE caudales_last.valor IS NOT NULL 
          GROUP BY caudales_last.unid, caudales_last.series_id
        ), ult_v AS (
         SELECT ult_1.unid,
            ult_1.date,
            caudales_last.valor
           FROM ult ult_1,
            caudales_last
          WHERE ult_1.unid = caudales_last.unid AND caudales_last.timestart = ult_1.date
        ), v AS (
         SELECT ult_v.unid,
            ult_v.date,
            ult_v.valor,
            round(avg(caudales_last.valor)::numeric, 2) AS valor_precedente
           FROM ult_v
             LEFT JOIN caudales_last ON ult_v.unid = caudales_last.unid AND caudales_last.timestart <= (ult_v.date - '1 day'::interval) AND caudales_last.timestart > (ult_v.date - '2 days'::interval)
          GROUP BY ult_v.unid, ult_v.date, ult_v.valor
        ), n AS (
         SELECT v.unid,
            v.date,
            v.valor,
            v.valor_precedente,
                CASE
                    WHEN round(v.valor::numeric, 2) > round(v.valor_precedente, 2) THEN 'crece'::text
                    WHEN round(v.valor::numeric, 2) = round(v.valor_precedente, 2) THEN 'permanece'::text
                    ELSE 'baja'::text
                END AS tendencia
           FROM v
          GROUP BY v.unid, v.date, v.valor, v.valor_precedente, (
                CASE
                    WHEN round(v.valor::numeric, 2) > round(v.valor_precedente, 2) THEN 'crece'::text
                    WHEN round(v.valor::numeric, 2) = round(v.valor_precedente, 2) THEN 'permanece'::text
                    ELSE 'baja'::text
                END)
          ORDER BY v.unid
        )
 SELECT n.unid,
    timezone('ART'::text, n.date) AS fecha,
    n.valor,
    n.valor_precedente,
    n.tendencia,
    estaciones.nombre,
    estaciones.tabla,
    estaciones.geom,
    ult.series_id,
    to_json(ult.timeseries)::text timeseries,
    ult.timestart,
    ult.timeend
   FROM n,
    estaciones,
    ult
  WHERE n.unid = estaciones.unid AND estaciones.unid = ult.unid
  ORDER BY estaciones.tabla, n.unid
