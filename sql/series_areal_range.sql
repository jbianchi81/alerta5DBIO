create or replace view series_areal_range AS 
SELECT series_areal.id AS id,
    series_areal.fuentes_id AS f_id,
    fuentes.nombre AS fuente,
    series_areal.area_id, 
    areas_pluvio.nombre AS area,
    count(observaciones_areal.timestart) AS count,
    min(observaciones_areal.timestart) AS min,
    max(observaciones_areal.timestart) AS max,
    series_rast.id AS srast_id
   FROM series_areal
   JOIN fuentes ON fuentes.id = series_areal.fuentes_id
   JOIN areas_pluvio ON areas_pluvio.unid = area_id
   JOIN series_rast ON series_rast.fuentes_id=fuentes.id
   LEFT JOIN observaciones_areal ON series_areal.id = observaciones_areal.series_id
  GROUP BY series_areal.id,
    series_areal.fuentes_id,
    fuentes.nombre,
    area_id, 
    areas_pluvio.nombre,
    series_rast.id
  ORDER BY fuentes.nombre, series_areal.id;