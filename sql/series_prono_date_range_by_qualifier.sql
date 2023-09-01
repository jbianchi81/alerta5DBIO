BEGIN;
DROP TABLE IF EXISTS series_puntual_prono_date_range_by_qualifier;
CREATE TABLE series_puntual_prono_date_range_by_qualifier (
    series_id integer not null references series(id),
    cor_id integer not null references corridas(id),
    qualifier varchar,
    begin_date timestamp not null,
    end_date timestamp not null,
    count integer not null,
    unique (series_id,cor_id,qualifier)
);

INSERT INTO series_puntual_prono_date_range_by_qualifier (series_id,cor_id,qualifier,begin_date,end_date,count)
SELECT series.id AS series_id,
    pronosticos.cor_id,
    pronosticos.qualifier,
    min(pronosticos.timestart) AS begin_date,
    max(pronosticos.timestart) AS end_date,
    count(pronosticos.timestart) AS count
   FROM estaciones,
    series,
    pronosticos
  WHERE estaciones.unid = series.estacion_id
  AND series.id = pronosticos.series_id
  GROUP BY series.id, pronosticos.cor_id, pronosticos.qualifier;

DROP TABLE IF EXISTS series_areal_prono_date_range_by_qualifier;
CREATE TABLE series_areal_prono_date_range_by_qualifier (
    series_id integer not null references series_areal(id),
    cor_id integer not null references corridas(id),
    qualifier varchar,
    begin_date timestamp not null,
    end_date timestamp not null,
    count integer not null,
    unique (series_id,cor_id,qualifier)
);

INSERT INTO series_areal_prono_date_range_by_qualifier (series_id,cor_id,qualifier,begin_date,end_date,count)
SELECT series_areal.id AS series_id,
    pronosticos_areal.cor_id,
    pronosticos_areal.qualifier,
    min(pronosticos_areal.timestart) AS begin_date,
    max(pronosticos_areal.timestart) AS end_date,
    count(pronosticos_areal.timestart) AS count
   FROM series_areal
   JOIN pronosticos_areal ON series_areal.id = pronosticos_areal.series_id
   JOIN areas_pluvio ON series_areal.area_id = areas_pluvio.unid
   LEFT JOIN estaciones ON areas_pluvio.exutorio_id = estaciones.unid
  GROUP BY series_areal.id, pronosticos_areal.cor_id, pronosticos_areal.qualifier
;

DROP MATERIALIZED VIEW IF EXISTS series_prono_date_range_by_qualifier;
DROP VIEW IF EXISTS series_prono_date_range_by_qualifier;
CREATE VIEW series_prono_date_range_by_qualifier AS
SELECT series.id AS series_id,
    'series' AS series_table,
    series.estacion_id,
    estaciones.tabla,
    series.var_id,
    corridas.id AS cor_id,
    series_puntual_prono_date_range_by_qualifier.qualifier,
    series_puntual_prono_date_range_by_qualifier.begin_date,
    series_puntual_prono_date_range_by_qualifier.end_date,
    series_puntual_prono_date_range_by_qualifier.count
   FROM corridas 
   JOIN series_puntual_prono_date_range_by_qualifier ON series_puntual_prono_date_range_by_qualifier.cor_id = corridas.id
   JOIN series ON series.id=series_puntual_prono_date_range_by_qualifier.series_id
   JOIN estaciones ON estaciones.unid = series.estacion_id

UNION ALL

SELECT series_areal.id AS series_id,
    'series_areal' AS series_table,
    series_areal.area_id AS estacion_id,
    estaciones.tabla,
    series_areal.var_id,
    corridas.id AS cor_id,
    series_areal_prono_date_range_by_qualifier.qualifier,
    series_areal_prono_date_range_by_qualifier.begin_date,
    series_areal_prono_date_range_by_qualifier.end_date,
    series_areal_prono_date_range_by_qualifier.count
   FROM corridas 
   JOIN series_areal_prono_date_range_by_qualifier ON series_areal_prono_date_range_by_qualifier.cor_id = corridas.id
   JOIN series_areal ON series_areal.id=series_areal_prono_date_range_by_qualifier.series_id
   JOIN areas_pluvio ON series_areal.area_id = areas_pluvio.unid
   LEFT JOIN estaciones ON areas_pluvio.exutorio_id = estaciones.unid
;

grant insert,select,update,delete on series_puntual_prono_date_range_by_qualifier to actualiza;
grant insert,select,update,delete on series_areal_prono_date_range_by_qualifier to actualiza;
grant select on series_prono_date_range_by_qualifier to actualiza;
-- grant select on series_prono_date_range to sololectura;


COMMIT;