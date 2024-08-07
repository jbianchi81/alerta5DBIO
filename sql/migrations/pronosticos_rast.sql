Begin;

CREATE TABLE public.pronosticos_rast (
    id integer NOT NULL,
    cor_id integer NOT NULL,
    series_id integer NOT NULL,
    timestart timestamp without time zone NOT NULL,
    timeend timestamp without time zone NOT NULL,
    qualifier character varying(50) DEFAULT 'main'::character varying,
    valor raster NOT NULL
);

CREATE SEQUENCE public.pronosticos_rast_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

ALTER SEQUENCE public.pronosticos_rast_id_seq OWNED BY public.pronosticos_rast.id;

ALTER TABLE ONLY public.pronosticos_rast ALTER COLUMN id SET DEFAULT nextval('public.pronosticos_rast_id_seq'::regclass);

ALTER TABLE ONLY public.pronosticos_rast
    ADD CONSTRAINT pronosticos_rast_cor_id_series_id_timestart_qualifier_key UNIQUE (cor_id, series_id, timestart, qualifier);

ALTER TABLE ONLY public.pronosticos_rast
    ADD CONSTRAINT pronosticos_rast_id_key UNIQUE (id);

ALTER TABLE ONLY public.pronosticos_rast
    ADD CONSTRAINT pronosticos_rast_cor_id_fkey FOREIGN KEY (cor_id) REFERENCES public.corridas(id) ON DELETE CASCADE;

ALTER TABLE ONLY public.pronosticos_rast
    ADD CONSTRAINT pronosticos_rast_series_id_fkey FOREIGN KEY (series_id) REFERENCES public.series_rast(id) ON DELETE CASCADE;

CREATE TABLE series_rast_prono_date_range_by_qualifier (
    series_id integer not null references series_rast(id) ON DELETE CASCADE,
    cor_id integer not null references corridas(id) ON DELETE CASCADE,
    qualifier varchar,
    begin_date timestamp not null,
    end_date timestamp not null,
    count integer not null,
    unique (series_id,cor_id,qualifier)
);

INSERT INTO series_rast_prono_date_range_by_qualifier (series_id,cor_id,qualifier,begin_date,end_date,count)
SELECT series_rast.id AS series_id,
    pronosticos_rast.cor_id,
    pronosticos_rast.qualifier,
    min(pronosticos_rast.timestart) AS begin_date,
    max(pronosticos_rast.timestart) AS end_date,
    count(pronosticos_rast.timestart) AS count
   FROM series_rast
   JOIN pronosticos_rast ON series_rast.id = pronosticos_rast.series_id
   GROUP BY series_rast.id, pronosticos_rast.cor_id, pronosticos_rast.qualifier
;

CREATE OR REPLACE VIEW series_prono_date_range_by_qualifier AS
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

UNION ALL

SELECT series_rast.id AS series_id,
    'series_rast' AS series_table,
    series_rast.escena_id AS estacion_id,
    null as tabla,
    series_rast.var_id,
    corridas.id AS cor_id,
    series_rast_prono_date_range_by_qualifier.qualifier,
    series_rast_prono_date_range_by_qualifier.begin_date,
    series_rast_prono_date_range_by_qualifier.end_date,
    series_rast_prono_date_range_by_qualifier.count
   FROM corridas 
   JOIN series_rast_prono_date_range_by_qualifier ON series_rast_prono_date_range_by_qualifier.cor_id = corridas.id
   JOIN series_rast ON series_rast.id=series_rast_prono_date_range_by_qualifier.series_id
;


commit;