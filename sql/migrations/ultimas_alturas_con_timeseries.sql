WITH alturas_last AS (
         SELECT series.estacion_id AS unid,
            observaciones.timestart,
            series.id AS series_id,
            series.var_id,
            valores_num.valor
           FROM estaciones_view,
            series,
            observaciones,
            valores_num
          WHERE estaciones_view.sitecode=series.estacion_id AND series.id = observaciones.series_id AND observaciones.id = valores_num.obs_id AND series.var_id = 2 AND series.proc_id = 1 AND series.unit_id = 11 AND valor!='NaN'
          AND observaciones.timestart >= case when '%timeStart%'='1800-01-01' 
                                        then current_timestamp-'7 days'::interval
                                        else '%timeStart%'::timestamp end
	AND observaciones.timeend <= case when '%timeEnd%' = '1800-01-01' 
	                                  then current_timestamp
	                                  else '%timeEnd%'::timestamp end
	      AND estaciones_view.redid= CASE WHEN %redid%!=0 THEN %redid% ELSE estaciones_view.redid END 
	      AND estaciones_view.tipo= CASE WHEN '%tipo%' != 'X' THEN '%tipo%' else estaciones_view.tipo END
        ), ult AS (
         SELECT alturas_last.unid,
            max(alturas_last.timestart) AS date,
            alturas_last.series_id,
            alturas_last.var_id,
            array_agg(array[to_char(timestart::timestamptz at time zone 'UTC','YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),round(valor::numeric,2)::text] order by timestart) AS timeseries,
            to_char(min(timestart::timestamptz at time zone 'UTC'),'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') timestart,
            to_char(max(timestart::timestamptz at time zone 'UTC'),'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') timeend
           FROM alturas_last
          WHERE alturas_last.valor IS NOT NULL
          AND alturas_last.valor != 'NaN'
          GROUP BY alturas_last.unid, alturas_last.series_id,
alturas_last.var_id
        ), ult_v AS (
         SELECT ult_1.unid,
            ult_1.date,
            alturas_last.valor
           FROM ult ult_1,
            alturas_last
          WHERE ult_1.unid = alturas_last.unid AND alturas_last.timestart = ult_1.date
        ), v AS (
         SELECT ult_v.unid,
            ult_v.date,
            ult_v.valor,
            round(avg(alturas_last.valor)::numeric, 2) AS valor_precedente
           FROM ult_v
             LEFT JOIN alturas_last ON ult_v.unid = alturas_last.unid AND alturas_last.timestart <= (ult_v.date - '1 day'::interval) AND alturas_last.timestart > (ult_v.date - '2 days'::interval)
          GROUP BY ult_v.unid, ult_v.date, ult_v.valor
        ), niveles_de_alerta as (
            SELECT 
				v.unid,
				CAST(alturas_alerta.valor as double precision) AS valor
			FROM
				v
			LEFT JOIN alturas_alerta 
			ON (v.unid = alturas_alerta.unid 
				AND estado='a')
		), niveles_de_evacuacion as (
            SELECT 
				v.unid,
				CAST(alturas_alerta.valor as double precision) AS valor
			FROM
				v
			LEFT JOIN alturas_alerta ON (
				v.unid = alturas_alerta.unid
				AND estado='e')
		), niveles_de_aguas_bajas as (
            SELECT 
				v.unid,
				CAST(alturas_alerta.valor as double precision) AS valor
			FROM
				v
			LEFT JOIN alturas_alerta ON (
				v.unid = alturas_alerta.unid
				AND estado='b')
		), n AS (
         SELECT v.unid,
            v.date,
            v.valor,
                CASE
					WHEN v.valor >= coalesce(max(niveles_de_evacuacion.valor::real),'Infinity'::real) THEN 'e'::text
					WHEN v.valor >= coalesce(max(niveles_de_alerta.valor::real),'Infinity'::real) THEN 'a'::text
					WHEN v.valor <= coalesce(max(niveles_de_aguas_bajas.valor::real),'-Infinity'::real) THEN 'b'::text
					WHEN (EXISTS ( SELECT 1
                       FROM alturas_alerta alturas_alerta_1
                      WHERE alturas_alerta_1.unid = v.unid)) THEN 'n'::text
                    ELSE 'x'::text
                END AS est,
            v.valor_precedente,
                CASE
                    WHEN round(v.valor::numeric, 2) > round(v.valor_precedente, 2) THEN 'crece'::text
                    WHEN round(v.valor::numeric, 2) = round(v.valor_precedente, 2) THEN 'permanece'::text
                    ELSE 'baja'::text
                END AS tendencia
           FROM v
             LEFT JOIN alturas_alerta ON alturas_alerta.unid = v.unid AND alturas_alerta.valor <= v.valor
             LEFT JOIN niveles_de_evacuacion ON v.unid = niveles_de_evacuacion.unid
             LEFT JOIN niveles_de_alerta ON v.unid = niveles_de_alerta.unid
             LEFT JOIN niveles_de_aguas_bajas ON v.unid = niveles_de_aguas_bajas.unid
             
          GROUP BY v.unid, v.date, v.valor, v.valor_precedente, (
                CASE
                    WHEN round(v.valor::numeric, 2) > round(v.valor_precedente, 2) THEN 'crece'::text
                    WHEN round(v.valor::numeric, 2) = round(v.valor_precedente, 2) THEN 'permanece'::text
                    ELSE 'baja'::text
                END)
          ORDER BY v.unid
        ),			
 percentiles AS (
    SELECT *
    FROM series_percentiles_ref
    UNION ALL (
        SELECT
            series_id,
            100,
            '-Infinity'::float8
        FROM series_percentiles_ref 
        WHERE percentil=5)
 )
 SELECT DISTINCT ON (estaciones.tabla, n.unid)
    n.unid,
    timezone('ART'::text, n.date) AS fecha,
    n.valor,
    n.valor_precedente,
    n.tendencia,
    CASE WHEN n.est='e' THEN 'evacuación'
         WHEN n.est='a' THEN 'alerta'
         WHEN n.est='n' THEN 'normal'
         WHEN n.est='b' THEN 'aguas bajas'
         ELSE '' 
    END estado,
    n.est,
    (n.tendencia || ':'::text) || n.est AS condicion,
    estaciones.nombre,
    estaciones.tabla,
    estaciones.geom,
    estaciones.cero_ign,
    ult.series_id,
    ult.var_id,
    to_json(ult.timeseries)::text timeseries,
    niveles_de_alerta.valor nivel_de_alerta,
    niveles_de_evacuacion.valor nivel_de_evacuacion,
    niveles_de_aguas_bajas.valor nivel_de_aguas_bajas,
    ult.timestart,
    ult.timeend,
    estaciones.rio,
    percentiles.percentil
   FROM n
   JOIN estaciones ON (n.unid = estaciones.unid)
   JOIN ult ON (estaciones.unid = ult.unid)
   JOIN niveles_de_alerta ON (niveles_de_alerta.unid=n.unid)
   JOIN niveles_de_evacuacion ON (niveles_de_evacuacion.unid=n.unid)
   JOIN niveles_de_aguas_bajas ON (niveles_de_aguas_bajas.unid=n.unid)
   LEFT JOIN percentiles ON (percentiles.series_id=ult.series_id AND n.valor >= percentiles.valor)
  ORDER BY estaciones.tabla asc, n.unid asc, percentiles.valor desc