WITH pronos AS (
            SELECT 
                series_areal_prono_date_range.series_id AS series_id,
                json_agg(
                    json_build_object(
                        'cal_id', corridas.cal_id,
                        'cal_grupo_id', calibrados.grupo_id,
                        'cor_id', series_areal_prono_date_range.cor_id,
                        'timestart', series_areal_prono_date_range.begin_date,
                        'timeend', series_areal_prono_date_range.end_date,
                        'count', series_areal_prono_date_range.count,
                        'qualifiers', series_areal_prono_date_range.qualifiers
                    )
                ) AS corridas_array
            FROM series_areal_prono_date_range
            JOIN corridas ON series_areal_prono_date_range.cor_id = corridas.id
            JOIN calibrados ON corridas.cal_id = calibrados.id
            WHERE
                ('%cal_grupo_id%' = 'NULL' OR calibrados.grupo_id = %cal_grupo_id%)
                AND 
                ('%cal_id%' = 'NULL' OR calibrados.id = %cal_id%)
            GROUP BY series_id
    ),
    series_json AS (
        SELECT
            series_areal.id,
            areas_pluvio.unid AS estacion_id,
            estaciones.tabla,
            json_build_object(
                'tipo', 'areal', 
                'id', series_areal.id, 
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
                'fuente', json_build_object(
                    'id', fuentes.id,
                    'nombre', fuentes.nombre,
                    'tipo', fuentes.tipo,
                    'hora_corte', fuentes.hora_corte,
                    'scale_factor', fuentes.scale_factor,
                    'data_offset', fuentes.data_offset,
                    'def_srid', fuentes.def_srid,
                    'def_extent', fuentes.def_extent,
                    'def_pixel_height', fuentes.def_pixel_height,
                    'def_pixel_width', fuentes.def_pixel_width,
                    'def_pixeltype', fuentes.def_pixeltype,
                    'abstract', fuentes.abstract,
                    'source', fuentes.source
                ),
                'date_range', json_build_object(
                    'timestart', series_areal_date_range.timestart,
                    'timeend', series_areal_date_range.timeend,
                    'count', series_areal_date_range.count),
                'pronosticos', pronos.corridas_array,
                'data_availability', case when series_areal_date_range.timeend is not null
                    then
                        case when now() - series_areal_date_range.timeend < '1 days'::interval
                                then case when pronos.corridas_array is not null
                                    then 9 -- 'RT+S'
                                    else 8 -- 'RT'
                                    end
                                when now() - series_areal_date_range.timeend < '3 days'::interval
                                then case when pronos.corridas_array is not null
                                    then 7 -- 'NRT+S'
                                    else 6 -- 'NRT'
                                    end
                                when (series_areal_date_range.timestart <= '%timeend%'::timestamp) and (series_areal_date_range.timeend >= '%timestart%'::timestamp)
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
        FROM areas_pluvio
            LEFT JOIN estaciones ON areas_pluvio.exutorio_id = estaciones.unid 
            LEFT JOIN redes on estaciones.tabla = redes.tabla_id
            JOIN series_areal ON series_areal.area_id = areas_pluvio.unid
            JOIN fuentes ON fuentes.id = series_areal.fuentes_id
            JOIN var ON series_areal.var_id = var.id
            JOIN procedimiento ON series_areal.proc_id = procedimiento.id
            JOIN unidades ON series_areal.unit_id = unidades.id
            LEFT JOIN series_areal_date_range ON series_areal.id = series_areal_date_range.series_id
            LEFT JOIN pronos ON pronos.series_id=series_areal.id
        WHERE
            series_areal.id=coalesce(%series_id%,series_areal.id)
            AND areas_pluvio.unid=coalesce(%estacion_id%,areas_pluvio.unid)
            AND procedimiento.id=coalesce(%proc_id%,procedimiento.id)
            AND var.id=coalesce(%var_id%,var.id)
            AND unidades.id=coalesce(%unit_id%,unidades.id)
            AND fuentes.id=coalesce(%fuentes_id%,fuentes.id)
            AND ('%GeneralCategory%'='NULL' OR var."GeneralCategory"='%GeneralCategory%')
            AND ('%tabla%'='NULL' OR estaciones.tabla='%tabla%')
            AND ('%red_id%'='NULL' OR redes.id=%red_id%)
            AND ('%id_externo%'='NULL' OR estaciones.id_externo=%id_externo%)
            AND ('%date_range_before%'='1900-01-01' OR (series_areal_date_range.timestart IS NOT NULL AND series_areal_date_range.timestart>='%date_range_before%'::timestamp))
            AND ('%date_range_after%'='2100-01-01' OR (series_areal_date_range.timeend IS NOT NULL AND series_areal_date_range.timeend<='%date_range_after%'::timestamp))
            AND ('%data_availability%'='NULL' 
                OR '%data_availability%'='a'
                OR ('%data_availability%'='r' AND series_areal_date_range.timeend IS NOT NULL AND now() - series_areal_date_range.timeend < '1 days'::interval) 
                OR ('%data_availability%'='n' AND series_areal_date_range.timeend IS NOT NULL AND now() - series_areal_date_range.timeend < '3 days'::interval) 
                OR ('%data_availability%'='c' AND series_areal_date_range.timeend IS NOT NULL AND (series_areal_date_range.timestart <= '%timeend%'::timestamp) AND (series_areal_date_range.timeend >= '%timestart%'::timestamp))
                OR ('%data_availability%'='h' AND series_areal_date_range.timeend IS NOT NULL AND series_areal_date_range.timeend IS NOT NULL)
            )
            AND (CAST ('%has_prono%' AS BOOLEAN) = false or pronos.corridas_array IS NOT NULL)
            AND ST_Intersects(areas_pluvio.geom, ST_MakeEnvelope(coalesce(%west%,-180), coalesce(%south%,-90), coalesce(%east%,180), coalesce(%north%,90), 4326))
    )
    SELECT 
        areas_pluvio.unid AS estacion_id, 
        areas_pluvio.nombre AS nombre, 
        areas_pluvio.exutorio AS exutorio, 
        areas_pluvio.exutorio_id AS exutorio_id,
        areas_pluvio.geom AS geom, 
        estaciones.tabla AS tabla,
        estaciones.pais AS pais,
        estaciones.rio AS rio, 
        redes.id as red_id, 
        redes.nombre as red_nombre,
        json_agg(series_json.serie) AS series,
        max((series_json.serie->>'data_availability')::int) data_availability
    FROM areas_pluvio
        LEFT JOIN estaciones ON areas_pluvio.exutorio_id = estaciones.unid
        LEFT JOIN redes ON estaciones.tabla::text = redes.tabla_id::text
        JOIN series_json ON series_json.estacion_id = areas_pluvio.unid
    GROUP BY 
        areas_pluvio.unid, 
        areas_pluvio.nombre, 
        areas_pluvio.exutorio, 
        areas_pluvio.exutorio_id, 
        areas_pluvio.geom,
        estaciones.tabla,
        estaciones.pais,
        estaciones.rio, 
        redes.id, 
        redes.nombre
    ORDER BY 
        areas_pluvio.unid;