BEGIN;

DROP MATERIALIZED VIEW IF EXISTS series_rast_json;
CREATE MATERIALIZED VIEW series_rast_json AS
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
    series_rast.id,
    json_build_object(
    'tipo','raster',
    'id',series_rast.id,
    'estacion',json_build_object(
        'id',escenas.id,
        'nombre',escenas.nombre,
        'geom',ST_ASGEOJSON(escenas.geom)::json
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
        'def_extent', fuentes.def_extent,
        'date_column', fuentes.date_column,
        'def_pixeltype', fuentes.def_pixeltype,
        'abstract', fuentes.abstract,
        'source', fuentes.source,
        'public', fuentes.public,
        'constraints', table_constraints_json.constraints
    ),
    'date_range', json_build_object(
        'timestart', series_rast_date_range.timestart,
        'timeend', series_rast_date_range.timeend,
        'count', series_rast_date_range.count
    )
)  AS serie
FROM series_rast
JOIN escenas ON (series_rast.escena_id = escenas.id)
JOIN var ON (series_rast.var_id = var.id)
JOIN procedimiento ON (series_rast.proc_id = procedimiento.id)
JOIN unidades ON (series_rast.unit_id = unidades.id)
JOIN fuentes ON (series_rast.fuentes_id = fuentes.id)
LEFT JOIN series_rast_date_range ON (series_rast.id = series_rast_date_range.series_id)
LEFT JOIN table_constraints_json ON (fuentes.data_table = table_constraints_json.table_name)
ORDER BY series_rast.id;

GRANT SELECT ON series_rast_json TO actualiza;
ALTER TABLE series_rast_json OWNER TO actualiza;

COMMIT;