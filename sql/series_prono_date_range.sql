BEGIN;
DROP TABLE IF EXISTS series_puntual_prono_date_range;
CREATE TABLE series_puntual_prono_date_range (
    series_id integer not null references series(id),
    cor_id integer not null references corridas(id),
    begin_date timestamp not null,
    end_date timestamp not null,
    count integer not null,
    qualifiers json not null,
    unique (series_id,cor_id)
);

INSERT INTO series_puntual_prono_date_range (series_id,cor_id,begin_date,end_date,count,qualifiers)
SELECT series.id AS series_id,
    pronosticos.cor_id,
    min(pronosticos.timestart) AS begin_date,
    max(pronosticos.timestart) AS end_date,
    count(pronosticos.timestart) AS count,
    json_agg(DISTINCT qualifier) AS qualifiers
   FROM estaciones
   JOIN series ON estaciones.unid = series.estacion_id
   JOIN pronosticos ON series.id = pronosticos.series_id
  GROUP BY series.id,pronosticos.cor_id;

DROP TABLE IF EXISTS series_areal_prono_date_range;
CREATE TABLE series_areal_prono_date_range (
    series_id integer not null references series_areal(id),
    cor_id integer not null references corridas(id),
    begin_date timestamp not null,
    end_date timestamp not null,
    count integer not null,
    qualifiers json not null,
    unique (series_id,cor_id)
);

INSERT INTO series_areal_prono_date_range (series_id,cor_id,begin_date,end_date,count,qualifiers)
SELECT series_areal.id AS series_id,
    pronosticos_areal.cor_id,
    min(pronosticos_areal.timestart) AS begin_date,
    max(pronosticos_areal.timestart) AS end_date,
    count(pronosticos_areal.timestart) AS count,
    json_agg(DISTINCT qualifier) AS qualifiers
   FROM series_areal
   JOIN pronosticos_areal ON series_areal.id = pronosticos_areal.series_id
   JOIN areas_pluvio ON series_areal.area_id = areas_pluvio.unid
   LEFT JOIN estaciones ON areas_pluvio.exutorio_id = estaciones.unid
  GROUP BY series_areal.id,pronosticos_areal.cor_id;

DROP MATERIALIZED VIEW IF EXISTS series_prono_date_range;
DROP VIEW IF EXISTS series_prono_date_range;
CREATE VIEW series_prono_date_range AS
SELECT series.id AS series_id,
    'series' AS series_table,
    series.estacion_id,
    estaciones.tabla,
    series.var_id,
    corridas.id AS cor_id,
    series_puntual_prono_date_range.begin_date,
    series_puntual_prono_date_range.end_date,
    series_puntual_prono_date_range.count,
    series_puntual_prono_date_range.qualifiers
   FROM corridas 
   JOIN series_puntual_prono_date_range ON series_puntual_prono_date_range.cor_id = corridas.id
   JOIN series ON series.id=series_puntual_prono_date_range.series_id
   JOIN estaciones ON estaciones.unid = series.estacion_id

UNION ALL

SELECT series_areal.id AS series_id,
    'series_areal' AS series_table,
    series_areal.area_id AS estacion_id,
    estaciones.tabla,
    series_areal.var_id,
    corridas.id AS cor_id,
    series_areal_prono_date_range.begin_date,
    series_areal_prono_date_range.end_date,
    series_areal_prono_date_range.count,
    series_areal_prono_date_range.qualifiers
   FROM corridas 
   JOIN series_areal_prono_date_range ON series_areal_prono_date_range.cor_id = corridas.id
   JOIN series_areal ON series_areal.id=series_areal_prono_date_range.series_id
   JOIN areas_pluvio ON series_areal.area_id = areas_pluvio.unid
   LEFT JOIN estaciones ON areas_pluvio.exutorio_id = estaciones.unid
;

grant insert,select,update,delete on series_puntual_prono_date_range to actualiza;
grant insert,select,update,delete on series_areal_prono_date_range to actualiza;
grant select on series_prono_date_range to actualiza;
-- grant select on series_prono_date_range to sololectura;


COMMIT;