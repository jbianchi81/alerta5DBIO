BEGIN;

CREATE TYPE series_prono_date_range_record AS (
    series_table varchar,
    series_id integer,
    cor_id integer,
    begin_date timestamp without time zone,
    end_date timestamp without time zone,
    count integer,
    qualifiers json
);

CREATE OR REPLACE FUNCTION update_series_puntual_prono_date_range(cor_id integer)
RETURNS setof series_prono_date_range_record
AS
$$
INSERT INTO series_puntual_prono_date_range 
    (
        series_id,
        cor_id,
        begin_date,
        end_date,
        count,
        qualifiers
    )
    SELECT series.id AS series_id,
        pronosticos.cor_id,
        min(pronosticos.timestart) AS begin_date,
        max(pronosticos.timestart) AS end_date,
        count(pronosticos.timestart) AS count,
        json_agg(DISTINCT qualifier) AS qualifiers
    FROM estaciones
    JOIN series ON estaciones.unid = series.estacion_id
    JOIN pronosticos ON series.id = pronosticos.series_id
    JOIN corridas ON corridas.id = pronosticos.cor_id
    WHERE corridas.id = $1
    GROUP BY series.id,pronosticos.cor_id
ON CONFLICT (series_id,cor_id) DO UPDATE SET
    begin_date=EXCLUDED.begin_date,
    end_date=EXCLUDED.end_date,
    count=EXCLUDED.count,
    qualifiers=EXCLUDED.qualifiers
RETURNING 
    'series'::varchar series_table,
    series_id,
    cor_id,
    begin_date,
    end_date,
    count,
    qualifiers;
$$
LANGUAGE SQL VOLATILE STRICT;

CREATE OR REPLACE FUNCTION update_series_areal_prono_date_range(cor_id integer)
RETURNS setof series_prono_date_range_record
AS
$$
INSERT INTO series_areal_prono_date_range 
    (
        series_id,
        cor_id,
        begin_date,
        end_date,
        count,
        qualifiers
        )
    SELECT series_areal.id AS series_id,
        pronosticos_areal.cor_id,
        min(pronosticos_areal.timestart) AS begin_date,
        max(pronosticos_areal.timestart) AS end_date,
        count(pronosticos_areal.timestart) AS count,
        json_agg(DISTINCT qualifier) AS qualifiers
    FROM series_areal
    JOIN pronosticos_areal ON series_areal.id = pronosticos_areal.series_id
    JOIN corridas ON corridas.id = pronosticos_areal.cor_id
    JOIN areas_pluvio ON series_areal.area_id = areas_pluvio.unid
    LEFT JOIN estaciones ON areas_pluvio.exutorio_id = estaciones.unid
    WHERE corridas.id = $1
    GROUP BY series_areal.id,pronosticos_areal.cor_id
ON CONFLICT (series_id,cor_id) 
DO UPDATE SET
    begin_date=EXCLUDED.begin_date,
    end_date=EXCLUDED.end_date,
    count=EXCLUDED.count,
    qualifiers=EXCLUDED.qualifiers
RETURNING 
    'series_areal'::varchar series_table,
    series_id,
    cor_id,
    begin_date,
    end_date,
    count,
    qualifiers;
$$
LANGUAGE SQL VOLATILE STRICT;

CREATE OR REPLACE FUNCTION update_series_rast_prono_date_range(cor_id integer)
RETURNS setof series_prono_date_range_record
AS
$$
INSERT INTO series_rast_prono_date_range_by_qualifier
    (
        series_id,
        cor_id,
        qualifier,
        begin_date,
        end_date,
        count
    )
    SELECT series_rast.id AS series_id,
        pronosticos_rast.cor_id,
        pronosticos_rast.qualifier,
        min(pronosticos_rast.timestart) AS begin_date,
        max(pronosticos_rast.timestart) AS end_date,
        count(pronosticos_rast.timestart) AS count
    FROM series_rast
    JOIN pronosticos_rast ON series_rast.id = pronosticos_rast.series_id
    WHERE pronosticos_rast.cor_id = $1
    GROUP BY series_rast.id, pronosticos_rast.cor_id, pronosticos_rast.qualifier
ON CONFLICT (series_id,cor_id,qualifier) 
DO UPDATE SET
    begin_date = EXCLUDED.begin_date,
    end_date = EXCLUDED.end_date,
    count = EXCLUDED.count
RETURNING 
    'series_rast'::varchar series_table,
    series_id,
    cor_id,
    begin_date,
    end_date,
    count,
    null::json;
$$
LANGUAGE SQL VOLATILE STRICT;



COMMIT;