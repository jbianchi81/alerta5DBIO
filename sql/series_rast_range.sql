create or replace view series_rast_range AS 
SELECT observaciones_rast.series_id,
    series_rast.nombre,
    count(observaciones_rast.timestart) AS count,
    min(observaciones_rast.timestart) AS min,
    max(observaciones_rast.timestart) AS max,
    series_rast.var_id,
    series_rast.unit_id,
    series_rast.proc_id,
    series_rast.fuentes_id
   FROM observaciones_rast
     JOIN series_rast ON series_rast.id = observaciones_rast.series_id
  GROUP BY observaciones_rast.series_id, series_rast.nombre, series_rast.var_id,
    series_rast.unit_id,
    series_rast.proc_id,
    series_rast.fuentes_id
  ORDER BY series_rast.nombre, observaciones_rast.series_id;