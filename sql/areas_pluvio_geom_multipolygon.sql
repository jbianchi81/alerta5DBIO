begin;

drop materialized view series_areal_json;

alter table areas_pluvio alter column geom type geometry(Geometry, 4326) using geom;

ALTER TABLE areas_pluvio
ADD CONSTRAINT geom_polygon_multipolygon_check
CHECK (GeometryType(geom) IN ('POLYGON', 'MULTIPOLYGON'));

CREATE MATERIALIZED VIEW series_areal_json AS
WITH table_constraints_json AS (
    SELECT
        table_constraints.table_name,
        json_build_object(
            'table_name', table_constraints.table_name,
            'constraints', array_agg(json_build_object(
                'constraint_name', table_constraints.constraint_name,
                'constraint_type', table_constraints.constraint_type
            ))
        ) AS constraints
    FROM table_constraints
    GROUP BY table_constraints.table_name )
SELECT
    series_areal.id,
    areas_pluvio.unid as estacion_id,
    areas_pluvio.nombre as nombre,
    estaciones.tabla as tabla,
    estaciones.id_externo as id_externo,
    estaciones.rio as rio,
    redes.id as red_id,
    var.id as var_id,
    var.nombre as var_nombre,
    var."GeneralCategory" as "GeneralCategory",
    procedimiento.id as proc_id,
    unidades.id as unit_id,
    fuentes.id as fuentes_id,
    fuentes.nombre as fuentes_nombre,
    fuentes.public as public,
    st_asgeojson(st_envelope(areas_pluvio.geom)) as extent,
    json_build_object(
    'tipo','areal',
    'id',series_areal.id,
    'estacion',json_build_object(
        'id',areas_pluvio.unid,
        'nombre',areas_pluvio.nombre,
        'geom',ST_ASGEOJSON(areas_pluvio.geom)::json,
        'exutorio',json_build_object(
            'id', estaciones.unid,
            'geom', ST_ASGEOJSON(estaciones.geom)::json,
            'tabla', estaciones.tabla
        )
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
    'fuente', json_build_object(
        'id', fuentes.id,
        'nombre', fuentes.nombre,
        'data_table', fuentes.data_table,
        'data_column', fuentes.data_column,
        'tipo', fuentes.tipo,
        'def_proc_id', fuentes.def_proc_id,
        'def_dt', fuentes.def_dt,
        'hora_corte', fuentes.hora_corte,
        'def_unit_id', fuentes.def_unit_id,
        'def_var_id', fuentes.def_var_id,
        'fd_column', fuentes.fd_column,
        'mad_table', fuentes.mad_table,
        'scale_factor', fuentes.scale_factor,
        'data_offset', fuentes.data_offset,
        'def_extent', ST_ASGeoJSON(fuentes.def_extent)::json,
        'date_column', fuentes.date_column,
        'def_pixeltype', fuentes.def_pixeltype,
        'abstract', fuentes.abstract,
        'source', fuentes.source,
        'public', fuentes.public,
        'constraints', table_constraints_json.constraints
    ),
    'date_range', json_build_object(
        'timestart', series_areal_date_range.timestart,
        'timeend', series_areal_date_range.timeend,
        'count', series_areal_date_range.count
    )
)  AS serie
FROM series_areal
JOIN areas_pluvio ON (series_areal.area_id = areas_pluvio.unid)
LEFT JOIN estaciones ON (areas_pluvio.exutorio_id = estaciones.unid)
LEFT JOIN redes ON (estaciones.tabla = redes.tabla_id)
JOIN var ON (series_areal.var_id = var.id)
JOIN procedimiento ON (series_areal.proc_id = procedimiento.id)
JOIN unidades ON (series_areal.unit_id = unidades.id)
JOIN fuentes ON (series_areal.fuentes_id = fuentes.id)
LEFT JOIN series_areal_date_range ON (series_areal.id = series_areal_date_range.series_id)
LEFT JOIN table_constraints_json ON (fuentes.data_table = table_constraints_json.table_name)
ORDER BY series_areal.id;

commit;