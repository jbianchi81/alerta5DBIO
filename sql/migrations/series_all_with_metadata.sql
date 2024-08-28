CREATE OR REPLACE VIEW series_union_all_with_names AS
WITH s_all AS (
   SELECT series.id,
      'puntual'::text AS tipo,
      series.estacion_id,
      series.var_id,
      series.proc_id,
      series.unit_id,
      NULL::integer AS fuentes_id,
      estaciones.nombre AS nombre,
      estaciones.tabla AS fuentes_nombre
      FROM series
      JOIN estaciones
         ON estaciones.unid = series.estacion_id
   UNION ALL
   SELECT series_areal.id,
      'areal'::text AS tipo,
      series_areal.area_id AS estacion_id,
      series_areal.var_id,
      series_areal.proc_id AS proc_id,
      series_areal.unit_id AS unit_id,
      series_areal.fuentes_id,
      areas_pluvio.nombre AS nombre,
      fuentes.nombre AS fuentes_nombre     
      FROM series_areal
      JOIN areas_pluvio
         ON areas_pluvio.unid = series_areal.area_id
      JOIN fuentes
         ON fuentes.id = series_areal.fuentes_id
   UNION ALL
   SELECT series_rast.id,
      'raster'::text AS tipo,
      series_rast.escena_id AS estacion_id,
      series_rast.var_id,
      series_rast.proc_id AS proc_id,
      series_rast.unit_id AS unid_id,
      series_rast.fuentes_id,
      escenas.nombre AS nombre,
      fuentes.nombre AS fuentes_nombre
      FROM series_rast
      JOIN escenas
         ON escenas.id = series_rast.escena_id
      JOIN fuentes
         ON fuentes.id = series_rast.fuentes_id
) 
SELECT
   s_all.*,
   var.var AS var_var,
   var.nombre AS var_nombre,
   var."timeSupport" AS "var_timeSupport",
   procedimiento.nombre AS proc_nombre,
   unidades.nombre AS unit_nombre
FROM s_all
JOIN var
   ON s_all.var_id = var.id
JOIN procedimiento
   ON s_all.proc_id = procedimiento.id
JOIN unidades
   ON s_all.unit_id = unidades.id;

