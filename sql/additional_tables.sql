\set ON_ERROR_STOP on

-- monitored_vars

CREATE TABLE public.monitored_vars (
    tipo character varying NOT NULL,
    id integer NOT NULL,
    nombre character varying NOT NULL,
    tipo2 character varying DEFAULT 'hidro'::character varying NOT NULL,
    CONSTRAINT tipo2_constraint CHECK ((((tipo2)::text = 'hidro'::text) OR ((tipo2)::text = 'meteo'::text)))
);


COPY public.monitored_vars (tipo, id, nombre, tipo2) FROM stdin;
puntual	2	altura	hidro
puntual	4	caudal	hidro
puntual	26	volumen útil	hidro
puntual	39	altura media diaria	hidro
puntual	40	caudal medio diario	hidro
areal	15	Evapotranspiración potencial	hidro
areal	20	Humedad del suelo	hidro
areal	21	Magnitud de inundación	hidro
areal	30	Area Saturada	hidro
areal	36	Altura de marea meteorológica	hidro
puntual	22	Caudal Afluente	hidro
puntual	23	Caudal Efluente	hidro
puntual	24	Caudal Vertido	hidro
puntual	25	Caudal Transferido	hidro
puntual	33	altura media mensual	hidro
puntual	48	caudal medio mensual	hidro
puntual	49	Altura hidrométrica mínima mensual	hidro
puntual	50	Altura hidrométrica máxima mensual	hidro
puntual	51	Caudal mínimo mensual	hidro
puntual	52	Caudal máximo mensual	hidro
puntual	67	Altura hidrométrica media semanal	hidro
puntual	68	Caudal Medio Diario Máximo	hidro
puntual	35	Altura de marea astronómica	hidro
puntual	36	Altura de marea meteorológica	hidro
puntual	69	Caudal Medio Diario Mínimo	hidro
puntual	73	Temperatura del agua	hidro
puntual	1	Precipitación diaria 12Z	meteo
puntual	27	Precipitación a intervalo nativo	meteo
puntual	31	Precipitación horaria	meteo
puntual	34	Precipitación 3-horaria	meteo
areal	1	precipitación diaria 12Z	meteo
areal	31	precipitación horaria	meteo
areal	34	precipitación 3 horaria	meteo
puntual	38	Precipitación acumulada	meteo
puntual	43	temperatura de rocío	meteo
puntual	9	Velocidad del viento máxima	meteo
puntual	14	Radiación solar	meteo
puntual	10	Velocidad del viento media	meteo
puntual	17	nubosidad media diaria	meteo
puntual	13	Heliofanía	meteo
puntual	53	Temperatura	meteo
puntual	55	Velocidad del viento	meteo
puntual	57	Dirección del viento	meteo
puntual	58	Humedad relativa	meteo
puntual	60	Presión barométrica	meteo
puntual	56	Velocidad del viento horaria	meteo
puntual	62	nubosidad	meteo
puntual	63	presión al nivel del mar	meteo
puntual	5	Temperatura mínima	meteo
puntual	7	Temperatura media	meteo
puntual	6	Temperatura máxima	meteo
puntual	18	presión al nivel del mar media diaria	meteo
puntual	12	Humedad relativa media diaria	meteo
puntual	16	Presión barométrica media diaria	meteo
puntual	11	dirección del viento modal diaria	meteo
puntual	54	Temperatura horaria	meteo
puntual	59	Humedad relativa media horaria	meteo
puntual	61	Presión barométrica media horaria	meteo
\.


ALTER TABLE ONLY public.monitored_vars
    ADD CONSTRAINT monitored_vars_tipo_id_key UNIQUE (tipo, id);


-- series_prono_last (view)

CREATE OR REPLACE VIEW series_prono_last AS
 WITH corridas_max_date AS (
         SELECT corridas.cal_id,
            max(corridas.date) AS date
           FROM corridas
          GROUP BY corridas.cal_id
        ), corridas_last AS (
         SELECT corridas.cal_id,
            corridas.date AS fecha_emision,
            corridas.id AS cor_id,
            calibrados.nombre,
            calibrados.modelo,
            calibrados.model_id,
            calibrados.public,
            calibrados.grupo_id
           FROM corridas,
            corridas_max_date,
            calibrados
          WHERE corridas.date = corridas_max_date.date AND corridas.cal_id = corridas_max_date.cal_id AND corridas.cal_id = calibrados.id
          ORDER BY corridas.cal_id
        ), series_prono_last AS (
         SELECT corridas_last.cal_id,
            corridas_last.fecha_emision,
            corridas_last.cor_id,
            corridas_last.nombre,
            corridas_last.modelo,
            corridas_last.model_id,
            corridas_last.public,
            corridas_last.grupo_id,
            pronosticos.series_id,
            min(pronosticos.timestart) AS timestart,
            max(pronosticos.timeend) AS timeend,
            count(pronosticos.timestart) AS count
           FROM corridas_last,
            pronosticos
          WHERE corridas_last.cor_id = pronosticos.cor_id
          GROUP BY corridas_last.cal_id, corridas_last.fecha_emision, corridas_last.cor_id, corridas_last.grupo_id, corridas_last.nombre, corridas_last.modelo, corridas_last.model_id, pronosticos.series_id, corridas_last.public
        )
 SELECT series_prono_last.cal_id,
    series_prono_last.fecha_emision,
    series_prono_last.cor_id,
    series_prono_last.nombre,
    series_prono_last.modelo,
    series_prono_last.model_id,
    series_prono_last.public,
    series_prono_last.grupo_id AS cal_grupo_id,
    series_prono_last.series_id,
    series_prono_last.timestart,
    series_prono_last.timeend,
    series_prono_last.count,
    estaciones.nombre AS estacion_nombre,
    estaciones.unid AS estacion_id,
    var.nombre AS var_nombre,
    var.id AS var_id
   FROM series_prono_last,
    series,
    estaciones,
    var
  WHERE series_prono_last.series_id = series.id AND series.estacion_id = estaciones.unid AND series.var_id = var.id
  ORDER BY series_prono_last.cal_id, var.id, estaciones.unid;

-- series_doy_stats

CREATE TABLE public.series_doy_stats (
    tipo character varying NOT NULL,
    series_id integer NOT NULL,
    doy integer NOT NULL,
    count integer,
    min real,
    max real,
    mean real,
    p01 real,
    p10 real,
    p50 real,
    p90 real,
    p99 real,
    window_size integer,
    timestart date,
    timeend date
);


ALTER TABLE ONLY public.series_doy_stats
    ADD CONSTRAINT series_doy_stats_tipo_series_id_doy_key UNIQUE (tipo, series_id, doy);


--
-- Name: series_mon_stats; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.series_mon_stats (
    tipo character varying NOT NULL,
    series_id integer NOT NULL,
    mon integer NOT NULL,
    count integer,
    min real,
    max real,
    mean real,
    p01 real,
    p10 real,
    p50 real,
    p90 real,
    p99 real,
    timestart timestamp without time zone,
    timeend timestamp without time zone
);


--
-- Name: series_mon_stats_tipo_series_id_mon_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.series_mon_stats
    ADD CONSTRAINT series_mon_stats_tipo_series_id_mon_key UNIQUE (tipo, series_id, mon);


--
-- Name: TABLE series_mon_stats; Type: ACL; Schema: public; Owner: -
--

-- GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE public.series_mon_stats TO actualiza;
-- GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE public.series_mon_stats TO sololectura;

create table series_percentiles_ref (
  series_id int references series(id), 
  percentil int not null, 
  valor real not null, 
  unique (series_id, percentil));


create table estaciones_cercanas (
    madre int references estaciones(unid) on delete cascade, 
    hija int references estaciones(unid) on delete cascade, 
    CONSTRAINT check_columns_diff CHECK (madre <> hija), 
    unique(madre, hija)
);

create view estaciones_cercanas_view as
with madres as (
    select
    estaciones_cercanas.*,estaciones.* 
    from estaciones_cercanas 
    join estaciones on madre=unid )
select  
  madres.madre,
  madres.nombre as madre_nombre,
  madres.tabla as madre_tabla, 
  estaciones.unid as hija,
  estaciones.nombre as hija_nombre, 
  estaciones.tabla as hija_tabla, 
  round(st_distance(madres.geom,estaciones.geom)::numeric,4) as distancia 
from madres 
join estaciones 
on madres.hija=estaciones.unid 
order by madres.madre;

-- \copy estaciones_cercanas (madre,hija) from 'estaciones_cercanas_parana.csv' with csv

-- with p as (select estacion_id,series_id,var_id,percentil,valor from series_percentiles_ref join series on series.id=series_id where series.var_id=4)  insert into series_percentiles_ref (series_id,percentil,valor) select series.id series_hija,percentil,valor from p join estaciones_cercanas on estaciones_cercanas.madre=p.estacion_id join series on estaciones_cercanas.hija=series.estacion_id where series.var_id in (40,48,102,87) and series.proc_id=1 order by series.estacion_id, series.var_id, p.valor;

-- with p as (select estacion_id,series_id,var_id,percentil,valor from series_percentiles_ref join series on series.id=series_id where series.var_id=2)  insert into series_percentiles_ref (series_id,percentil,valor) select series.id series_hija,percentil,valor from p join estaciones_cercanas on estaciones_cercanas.madre=p.estacion_id join series on estaciones_cercanas.hija=series.estacion_id where series.var_id in (33,39,67,85,101) and series.proc_id=1 on conflict (series_id,percentil) do nothing;

-- redes_accessors --

CREATE TABLE public.redes_accessors (
    id integer NOT NULL,
    tipo character varying NOT NULL,
    tabla_id character varying NOT NULL,
    var_id integer NOT NULL,
    accessor character varying,
    asociacion boolean DEFAULT false,
    CONSTRAINT redes_accessors_check CHECK ((((accessor IS NULL) AND (asociacion = true)) OR ((accessor IS NOT NULL) AND (asociacion = false)))),
    CONSTRAINT redes_accessors_tipo_check CHECK ((((tipo)::text = 'puntual'::text) OR ((tipo)::text = 'areal'::text) OR ((tipo)::text = 'raster'::text)))
);

CREATE SEQUENCE public.redes_accessors_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

ALTER SEQUENCE public.redes_accessors_id_seq OWNED BY public.redes_accessors.id;

ALTER TABLE ONLY public.redes_accessors ALTER COLUMN id SET DEFAULT nextval('public.redes_accessors_id_seq'::regclass);

ALTER TABLE ONLY public.redes_accessors
    ADD CONSTRAINT redes_accessors_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.redes_accessors
    ADD CONSTRAINT redes_accessors_tipo_tabla_id_var_id_key UNIQUE (tipo, tabla_id, var_id);

ALTER TABLE ONLY public.redes_accessors
    ADD CONSTRAINT redes_accessors_accessor_fkey FOREIGN KEY (accessor) REFERENCES public.accessors(name);

ALTER TABLE ONLY public.redes_accessors
    ADD CONSTRAINT redes_accessors_tabla_id_fkey FOREIGN KEY (tabla_id) REFERENCES public.redes(tabla_id);

ALTER TABLE ONLY public.redes_accessors
    ADD CONSTRAINT redes_accessors_var_id_fkey FOREIGN KEY (var_id) REFERENCES public.var(id);


