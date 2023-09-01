WITH pronos AS (
            SELECT 
                series_puntual_prono_date_range.series_id AS series_id,
                json_agg(
                    json_build_object(
                        'cal_id', corridas.cal_id,
                        'cal_grupo_id', calibrados.grupo_id,
                        'cor_id', series_puntual_prono_date_range.cor_id,
                        'timestart', series_puntual_prono_date_range.begin_date,
                        'timeend', series_puntual_prono_date_range.end_date,
                        'count', series_puntual_prono_date_range.count,
                        'qualifiers', series_puntual_prono_date_range.qualifiers
                    )
                ) AS corridas_array
            FROM series_puntual_prono_date_range
            JOIN corridas ON series_puntual_prono_date_range.cor_id = corridas.id
            JOIN calibrados ON corridas.cal_id = calibrados.id
            WHERE
                ('%cal_grupo_id%' = 'NULL' OR calibrados.grupo_id = %cal_grupo_id%)
                AND 
                ('%cal_id%' = 'NULL' OR calibrados.id = %cal_id%)
            GROUP BY series_id
    ),
    series_json AS (
        SELECT
            series.id,
            estaciones.unid AS estacion_id,
            estaciones.tabla,
            json_build_object(
                'tipo', 'puntual', 
                'id', series.id, 
                'var', json_build_object(
                    'id', var.id, 
                    'var', var.var, 
                    'nombre', var.nombre, 
                    'abrev', var.abrev, 
                    'type', var.type, 
                    'datatype', var.datatype, 
                    'valuetype', var.valuetype, 
                    'GeneralCategory', var."GeneralCategory", 
                    'VariableName', var."VariableName", 
                    'SampleMedium', var."SampleMedium", 
                    'def_unit_id', var.def_unit_id, 
                    'timeSupport', var."timeSupport", 
                    'def_hora_corte', var.def_hora_corte),
                'procedimiento', json_build_object(
                    'id', procedimiento.id, 
                    'nombre', procedimiento.nombre, 
                    'abrev', procedimiento.abrev, 'descripcion', 
                    procedimiento.descripcion),
                'unidades', json_build_object(
                    'id', unidades.id, 
                    'nombre', unidades.nombre, 
                    'abrev', unidades.abrev, 
                    'UnitsID', unidades."UnitsID", 
                    'UnitsType', unidades."UnitsType"), 
                'fuente', json_build_object(),
                'date_range', json_build_object(
                    'timestart', series_date_range.timestart,
                    'timeend', series_date_range.timeend,
                    'count', series_date_range.count),
                'pronosticos', pronos.corridas_array,
                'data_availability', case when series_date_range.timeend is not null
                    then
                        case when now() - series_date_range.timeend < '1 days'::interval
                                then case when pronos.corridas_array is not null
                                    then 9 -- 'RT+S'
                                    else 8 -- 'RT'
                                    end
                                when now() - series_date_range.timeend < '3 days'::interval
                                then case when pronos.corridas_array is not null
                                    then 7 -- 'NRT+S'
                                    else 6 -- 'NRT'
                                    end
                                when (series_date_range.timestart <= '%timeend%'::timestamp) and (series_date_range.timeend >= '%timestart%'::timestamp)
                                then case when pronos.corridas_array is not null
                                    then 5 --'C+S'
                                    else 4 -- 'C'
                                    end
                                else case when pronos.corridas_array is not null
                                    then 3 -- 'H+S'
                                    else 2 -- 'H'
                                    end
                        end
                    when pronos.corridas_array is not null
                    then 1 -- 'S'
                    else 0 -- 'N'
                end
            ) AS serie
        FROM estaciones
            JOIN redes ON estaciones.tabla = redes.tabla_id
            JOIN series ON series.estacion_id = estaciones.unid
            JOIN var ON series.var_id = var.id
            JOIN procedimiento ON series.proc_id = procedimiento.id
            JOIN unidades ON series.unit_id = unidades.id
            LEFT JOIN series_date_range ON series.id = series_date_range.series_id
            LEFT JOIN pronos ON pronos.series_id=series.id
        WHERE
            series.id=coalesce(%series_id%,series.id)
            AND estaciones.unid=coalesce(%estacion_id%,estaciones.unid)
            AND procedimiento.id=coalesce(%proc_id%,procedimiento.id)
            AND var.id=coalesce(%var_id%,var.id)
            AND unidades.id=coalesce(%unit_id%,unidades.id)
            AND ('%GeneralCategory%'='NULL' OR var."GeneralCategory"='%GeneralCategory%')
            AND ('%tabla%'='NULL' OR estaciones.tabla='%tabla%')
            AND redes.id=coalesce(%red_id%,redes.id)
            AND estaciones.id_externo=coalesce(%id_externo%,estaciones.id_externo)
            AND ('%date_range_before%'='1900-01-01' OR (series_date_range.timestart IS NOT NULL AND series_date_range.timestart>='%date_range_before%'::timestamp))
            AND ('%date_range_after%'='2100-01-01' OR (series_date_range.timeend IS NOT NULL AND series_date_range.timeend<='%date_range_after%'::timestamp))
            AND ('%data_availability%'='NULL' 
                OR '%data_availability%'='a'
                OR ('%data_availability%'='r' AND series_date_range.timeend IS NOT NULL AND now() - series_date_range.timeend < '1 days'::interval) 
                OR ('%data_availability%'='n' AND series_date_range.timeend IS NOT NULL AND now() - series_date_range.timeend < '3 days'::interval) 
                OR ('%data_availability%'='c' AND series_date_range.timeend IS NOT NULL AND (series_date_range.timestart <= '%timeend%'::timestamp) AND (series_date_range.timeend >= '%timestart%'::timestamp))
                OR ('%data_availability%'='h' AND series_date_range.timeend IS NOT NULL AND series_date_range.timeend IS NOT NULL)
            )
            AND (CAST ('%has_prono%' AS BOOLEAN) = false or pronos.corridas_array IS NOT NULL)
            AND st_x(estaciones.geom) BETWEEN coalesce(%west%,-180) AND coalesce(%east%,%west%,180) AND st_y(estaciones.geom) BETWEEN coalesce(%south%,-90) AND coalesce(%north%,%south%,90)
    )
    SELECT 
        estaciones.unid AS estacion_id, 
        estaciones.nombre AS nombre, 
        estaciones.id_externo AS id_externo, 
        estaciones.geom AS geom, 
        estaciones.tabla AS tabla,
        estaciones.pais AS pais,
        estaciones.rio AS rio, 
        estaciones.has_obs AS has_obs, 
        estaciones.tipo AS tipo, 
        estaciones.automatica AS automatica, 
        estaciones.habilitar AS habilitar, 
        estaciones.propietario AS propietario, 
        estaciones.abrev AS abreviatura,
        estaciones.localidad AS localidad, 
        estaciones."real" AS real, 
        nivel_alerta.valor AS nivel_alerta, 
        nivel_evacuacion.valor AS nivel_evacuacion, 
        nivel_aguas_bajas.valor AS nivel_aguas_bajas,
        estaciones.altitud AS altitud,
        redes.public AS public, 
        estaciones.cero_ign AS cero_ign, 
        redes.id as red_id, 
        redes.nombre as red_nombre,
        json_agg(series_json.serie) AS series,
        max((series_json.serie->>'data_availability')::int) data_availability
    FROM estaciones
        JOIN redes ON estaciones.tabla::text = redes.tabla_id::text
        JOIN series_json ON series_json.estacion_id = estaciones.unid
        LEFT JOIN alturas_alerta nivel_alerta ON estaciones.unid = nivel_alerta.unid AND nivel_alerta.estado::text = 'a'::text
        LEFT JOIN alturas_alerta nivel_evacuacion ON estaciones.unid = nivel_evacuacion.unid AND nivel_evacuacion.estado::text = 'e'::text
        LEFT JOIN alturas_alerta nivel_aguas_bajas ON estaciones.unid = nivel_aguas_bajas.unid AND nivel_aguas_bajas.estado::text = 'b'::text
    GROUP BY 
        estaciones.unid, 
        estaciones.nombre, 
        estaciones.id_externo, 
        estaciones.geom,
        estaciones.tabla,
        estaciones.pais,
        estaciones.rio, 
        estaciones.has_obs, 
        estaciones.tipo, 
        estaciones.automatica, 
        estaciones.habilitar, 
        estaciones.propietario, 
        estaciones.abrev,
        estaciones.localidad, 
        estaciones."real", 
        nivel_alerta.valor, 
        nivel_evacuacion.valor, 
        nivel_aguas_bajas.valor,
        estaciones.altitud,
        redes.public, 
        estaciones.cero_ign, 
        redes.id, 
        redes.nombre
    ORDER BY 
        estaciones.unid;