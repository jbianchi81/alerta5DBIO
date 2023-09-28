WITH last_cor as (
    SELECT
            max(corridas_series.cor_id) cor_id,
            corridas_series.cal_id,
            corridas_series.series_id
        FROM corridas_series, calibrados
        WHERE corridas_series.cal_id=calibrados.id 
        AND corridas_series.var_id=%varId%
        AND corridas_series.proc_id in (4,8)
        AND calibrados.public=true
        GROUP BY corridas_series.cal_id,
                 corridas_series.series_id
                ),
          last_cor_data as ( 
                SELECT last_cor.cor_id,
                           last_cor.cal_id, 
                           last_cor.series_id,
                           calibrados.nombre calibrados_nombre, 
                           calibrados.modelo modelos_nombre, 
                           corridas.date fecha_emision,
                           estaciones.unid estacion_id,
                           redes.nombre red_nombre,
                           redes.id red_id,
                           redes.tabla_id red_code,
                           estaciones.id_externo,
                           estaciones.nombre estacion_nombre,
                           estaciones.geom,
                           series.var_id var_id,
                           var.abrev var_abrev,
                           series.unit_id unit_id,
                           unidades.abrev unit_abrev,
               estaciones.cero_ign,
                                          niveles_de_alerta.valor nivel_de_alerta,
                           niveles_de_evacuacion.valor nivel_de_evacuacion,
                           niveles_de_aguas_bajas.valor nivel_de_aguas_bajas
                FROM last_cor
                JOIN corridas ON (last_cor.cor_id=corridas.id )
                JOIN calibrados ON (last_cor.cal_id=calibrados.id)
                JOIN series ON (last_cor.series_id=series.id)
                JOIN var ON (series.var_id = var.id)
                JOIN unidades ON (series.unit_id = unidades.id)
                JOIN estaciones ON (series.estacion_id = estaciones.unid)
                JOIN redes ON (estaciones.tabla = redes.tabla_id)
                LEFT JOIN alturas_alerta niveles_de_alerta ON (niveles_de_alerta.unid = estaciones.unid and niveles_de_alerta.estado='a')
                LEFT JOIN alturas_alerta niveles_de_evacuacion ON (niveles_de_evacuacion.unid = estaciones.unid and niveles_de_evacuacion.estado='e')
                LEFT JOIN alturas_alerta niveles_de_aguas_bajas ON (niveles_de_aguas_bajas.unid = estaciones.unid and niveles_de_aguas_bajas.estado='b')
   
                ),
ts_data as (
        SELECT  last_cor_data.cor_id,
                        last_cor_data.series_id,
                        count(valores_prono_num.valor) n_obs,
                        array_agg(array[to_char(timestart::timestamptz at time zone 'UTC','YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),round(valores_prono_num.valor::numeric,2)::text,pronosticos.qualifier] order by timestart) AS valor,
                        max(valores_prono_num.valor) as max_valor,
                        to_char(min(timestart::timestamptz at time zone 'UTC'),'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') timestart,
                        to_char(max(timestart::timestamptz at time zone 'UTC'),'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') timeend
        FROM last_cor_data
                 JOIN pronosticos ON ( last_cor_data.cor_id = pronosticos.cor_id 
                    AND last_cor_data.series_id = pronosticos.series_id
                    AND pronosticos.timestart >= case when '%timeStart%'='1800-01-01' 
                                                then current_timestamp-'7 days'::interval
                                                else '%timeStart%'::timestamp end
                    AND pronosticos.timeend <= case when '%timeEnd%' = '1800-01-01' 
                                            then current_timestamp+'14 days'::interval
                                            else '%timeEnd%'::timestamp end
                    AND timestart>=current_date-'1 month'::interval)
                 JOIN valores_prono_num ON (pronosticos.id = valores_prono_num.prono_id)
        GROUP BY last_cor_data.cor_id,
                         last_cor_data.series_id
),
    b AS (
  SELECT last_cor_data.*,
          ts_data.n_obs,
          to_json(ts_data.valor)::text timeseries,
          ts_data.timestart,
          ts_data.timeend,
          case when ts_data.valor[1][2]::real-ts_data.valor[array_length(valor,1)][2]::real > 0.01 then 'baja'  when ts_data.valo
r[1][2]::real-ts_data.valor[array_length(valor,1)][2]::real > -0.01 then 'permanece' else 'crece' end AS tendencia,
      case when var_id in (2,67) then 
         case when nivel_de_alerta is null then 'x'
         when max_valor < coalesce(nivel_de_aguas_bajas,-9999) then 'l'
         when max_valor < nivel_de_alerta then 'n'
         when nivel_de_evacuacion is null then 'a'
         when max_valor < nivel_de_evacuacion then 'a'
         else 'e'
                end 
          else 'x'
          end AS estado,
    ts_data.valor[ts_data.n_obs][1] ultima_fecha, 
    ts_data.valor[ts_data.n_obs][2] ultimo_valor
    FROM last_cor_data
        JOIN ts_data ON (last_cor_data.cor_id = ts_data.cor_id
                                 AND last_cor_data.series_id = ts_data.series_id)
  )
  SELECT 
  b.*,
  (b.tendencia || ':'::text) || b.estado AS condicion
FROM b
ORDER BY estacion_id
