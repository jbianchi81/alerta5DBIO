begin;
drop view asociaciones_view;
create view asociaciones_view AS
WITH s_all AS (
         SELECT 'puntual'::text AS tipo,
            series.id,
            estaciones.unid AS sitio_id,
            estaciones.tabla AS fuentes_id,
            series.var_id,
            series.proc_id,
            series.unit_id
           FROM estaciones,
            series
          WHERE estaciones.unid = series.estacion_id
        UNION ALL
         SELECT 'areal'::text AS tipo,
            series_areal.id,
            series_areal.area_id AS sitio_id,
            series_areal.fuentes_id::text AS fuentes_id,
            series_areal.var_id,
            series_areal.proc_id,
            series_areal.unit_id
           FROM series_areal
        UNION ALL
         SELECT 'raster'::text AS tipo,
            series_rast.id,
            series_rast.escena_id AS sitio_id,
            series_rast.fuentes_id::text AS fuentes_id,
            series_rast.var_id,
            series_rast.proc_id,
            series_rast.unit_id
           FROM series_rast
        )
 SELECT a.id,
    a.source_tipo,
    a.source_series_id,
    a.dest_tipo,
    a.dest_series_id,
    a.agg_func,
    a.dt,
    COALESCE(a.t_offset, '00:00:00'::interval) AS t_offset,
    a."precision",
    a.source_time_support,
    a.source_is_inst,
    s_source.sitio_id AS source_estacion_id,
    s_source.fuentes_id AS source_fuentes_id,
    s_source.var_id AS source_var_id,
    s_source.proc_id AS source_proc_id,
    s_source.unit_id AS source_unit_id,
    s_dest.sitio_id AS dest_estacion_id,
    s_dest.fuentes_id AS dest_fuentes_id,
    s_dest.var_id AS dest_var_id,
    s_dest.proc_id AS dest_proc_id,
    s_dest.unit_id AS dest_unit_id,
    a.habilitar,
    a.expresion,
    a.cal_id,
    calibrados.nombre AS cal_nombre,
    var."timeSupport" AS dest_time_support
   FROM asociaciones a
     JOIN s_all s_source ON a.source_tipo::text = s_source.tipo AND a.source_series_id = s_source.id
     JOIN s_all s_dest ON a.dest_tipo::text = s_dest.tipo AND a.dest_series_id = s_dest.id
     JOIN var ON s_dest.var_id = var.id 
     LEFT JOIN calibrados ON calibrados.id = a.cal_id
  ORDER BY a.id;

-- grant all on asociaciones_view to actualiza;
commit;