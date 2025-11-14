BEGIN;

update fuentes set data_table='pp_persiann', date_column='date' WHERE id=51;

CREATE OR REPLACE VIEW pp_persiann AS
 SELECT
    o.id AS id, 
    o.timestart::date AS date,
    o.timestart AS timestart,
    o.timeend AS timeend,
    o.valor AS rast,
    o.timeupdate AS timeupdate
     FROM observaciones_rast AS o
     WHERE series_id=21
;

GRANT SELECT ON pp_persiann TO actualiza, sololectura;