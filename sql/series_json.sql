BEGIN;
DROP MATERIALIZED VIEW IF EXISTS series_json;
CREATE MATERIALIZED VIEW series_json AS
SELECT
    series.id,
    json_build_object(
    'tipo','puntual',
    'id',series.id,
    'estacion',json_build_object(
        'id',estaciones.unid,
        'nombre',estaciones.nombre,
        'id_externo',estaciones.id_externo,
        'geom',ST_ASGEOJSON(estaciones.geom)::json,
        'tabla', estaciones.tabla,
        'pais', estaciones.pais,
        'rio', estaciones.rio,
        'has_obs', estaciones.has_obs,
        'tipo', estaciones.tipo,
        'automatica', estaciones.automatica,
        'habilitar', estaciones.habilitar,
        'propietario', estaciones.propietario,
        'abreviatura', estaciones.abrev,
        'localidad', estaciones.localidad,
        'real', estaciones.real,
        'nivel_alerta', nivel_alerta.valor,
        'nivel_evacuacion', nivel_evacuacion.valor,
        'nivel_aguas_bajas', nivel_aguas_bajas.valor,
        'altitud', estaciones.altitud,
        'public', redes.public,
        'cero_ign', estaciones.cero_ign,
        'red_id', redes.id,
        'red_nombre', redes.nombre
    ),
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
        'def_hora_corte', var.def_hora_corte
    ),
        'procedimiento', json_build_object(
        'id', procedimiento.id,
        'nombre', procedimiento.nombre,
        'abrev', procedimiento.abrev,
        'descripcion', procedimiento.descripcion
    ),
    'unidades', json_build_object(
        'id', unidades.id,
        'nombre', unidades.nombre,
        'abrev', unidades.abrev,
        'UnitsID', unidades."UnitsID",
        'UnitsType', unidades."UnitsType"
    ),
    'fuente', json_build_object(),
    'date_range', json_build_object(
        'timestart', series_date_range.timestart,
        'timeend', series_date_range.timeend,
        'count', series_date_range.count
    )
)  AS serie
FROM series
JOIN  estaciones ON (series.estacion_id=estaciones.unid)
JOIN redes ON (estaciones.tabla=redes.tabla_id)
LEFT JOIN alturas_alerta  AS nivel_alerta ON (estaciones.unid=nivel_alerta.unid AND nivel_alerta.estado='a')
LEFT JOIN alturas_alerta AS nivel_evacuacion ON (estaciones.unid=nivel_evacuacion.unid AND nivel_evacuacion.estado='e')
LEFT JOIN  alturas_alerta  AS nivel_aguas_bajas ON (estaciones.unid=nivel_aguas_bajas.unid AND nivel_aguas_bajas.estado='b')
JOIN var ON (series.var_id = var.id)
JOIN procedimiento ON (series.proc_id = procedimiento.id)
JOIN unidades ON (series.unit_id = unidades.id)
LEFT JOIN series_date_range ON (series.id = series_date_range.series_id)
ORDER BY series.id;

GRANT SELECT ON series_json TO actualiza;
ALTER TABLE series_json OWNER TO actualiza;

COMMIT;