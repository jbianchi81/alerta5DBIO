\set ON_ERROR_STOP on

BEGIN;
CREATE TABLE public.observaciones_guardadas (
    id bigint NOT NULL,
    series_id integer,
    timestart timestamp without time zone,
    timeend timestamp without time zone,
    nombre character varying,
    descripcion character varying,
    unit_id integer,
    timeupdate timestamp without time zone DEFAULT now(),
    valor real not null
);

CREATE TABLE public.observaciones_areal_guardadas (
    id integer NOT NULL,
    series_id integer NOT NULL,
    timestart timestamp without time zone,
    timeend timestamp without time zone,
    nombre character varying,
    descripcion character varying,
    unit_id integer,
    timeupdate timestamp without time zone DEFAULT now(),
    valor real not null
);


ALTER TABLE ONLY public.observaciones_areal_guardadas
    ADD CONSTRAINT observaciones_areal_guardadas_pkey PRIMARY KEY (id);


--
-- Name: observaciones_areal_guardadas_series_id_timestart_timeend_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.observaciones_areal_guardadas
    ADD CONSTRAINT observaciones_areal_guardadas_series_id_timestart_timeend_key UNIQUE (series_id, timestart, timeend);


--
-- Name: observaciones_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.observaciones_guardadas
    ADD CONSTRAINT observaciones_guardadas_pkey PRIMARY KEY (id);


--
-- Name: observaciones_series_id_timestart_timeend_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.observaciones_guardadas
    ADD CONSTRAINT observaciones_guardadas_series_id_timestart_timeend_key UNIQUE (series_id, timestart, timeend);


ALTER TABLE ONLY public.observaciones_areal_guardadas
    ADD CONSTRAINT observaciones_areal_guardadas_series_id_fkey FOREIGN KEY (series_id) REFERENCES public.series_areal(id);

ALTER TABLE ONLY public.observaciones_areal_guardadas
    ADD CONSTRAINT observaciones_areal_guardadas_unit_id_fkey FOREIGN KEY (unit_id) REFERENCES public.unidades(id);

ALTER TABLE ONLY public.observaciones_guardadas
    ADD CONSTRAINT observaciones_guardadas_series_id_fkey FOREIGN KEY (series_id) REFERENCES public.series(id) ON DELETE CASCADE;


--
-- Name: observaciones_unit_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.observaciones_guardadas
    ADD CONSTRAINT observaciones_guardadas_unit_id_fkey FOREIGN KEY (unit_id) REFERENCES public.unidades(id);

CREATE TABLE public.observaciones_rast_guardadas (
    id integer NOT NULL,
    series_id integer NOT NULL,
    timestart timestamp without time zone NOT NULL,
    timeend timestamp without time zone NOT NULL,
    valor public.raster NOT NULL,
    timeupdate timestamp without time zone DEFAULT now() NOT NULL
);

ALTER TABLE ONLY public.observaciones_rast_guardadas
    ADD CONSTRAINT observaciones_rast_guardadas_pkey PRIMARY KEY (id);


--
-- Name: observaciones_rast_guardadas observaciones_rast_guardadas_series_id_timestart_timeend_key; Type: CONSTRAINT; Schema: public; Owner: alerta5
--

ALTER TABLE ONLY public.observaciones_rast_guardadas
    ADD CONSTRAINT observaciones_rast_guardadas_series_id_timestart_timeend_key UNIQUE (series_id, timestart, timeend);


ALTER TABLE ONLY public.observaciones_rast_guardadas
    ADD CONSTRAINT observaciones_rast_guardadas_series_id_fkey FOREIGN KEY (series_id) REFERENCES public.series_rast(id);


--
-- materialized views --
--

create materialized view series_guardadas_date_range AS
SELECT series.id AS series_id,
    min(timestart) AS timestart,
    max(timestart) AS timeend,
    count(timestart) AS count
   FROM series,
    observaciones_guardadas
  WHERE series.id = observaciones_guardadas.series_id
  GROUP BY series.id
  ORDER BY series.id;

create materialized view series_areal_guardadas_date_range AS
SELECT series_areal.id AS series_id,
    min(timestart) AS timestart,
    max(timestart) AS timeend,
    count(timestart) AS count
   FROM series_areal,
    observaciones_areal_guardadas
  WHERE series_areal.id = observaciones_areal_guardadas.series_id
  GROUP BY series_areal.id
  ORDER BY series_areal.id;

create materialized view series_rast_guardadas_date_range AS
SELECT series_rast.id AS series_id,
    min(timestart) AS timestart,
    max(timestart) AS timeend,
    count(timestart) AS count
   FROM series_rast,
    observaciones_rast_guardadas
  WHERE series_rast.id = observaciones_rast_guardadas.series_id
  GROUP BY series_rast.id
  ORDER BY series_rast.id;

alter materialized view series_date_range owner to matviews;
alter materialized view series_guardadas_date_range owner to matviews;
alter materialized view series_areal_date_range owner to matviews;
alter materialized view series_areal_guardadas_date_range owner to matviews;
alter materialized view series_rast_date_range owner to matviews;
alter materialized view series_rast_guardadas_date_range owner to matviews;
alter materialized view series_json owner to matviews;
alter materialized view series_areal_json owner to matviews;
alter materialized view series_areal_json_no_geom owner to matviews;
alter materialized view series_rast_json owner to matviews;
grant select on fuentes,areas_pluvio,unidades,procedimiento,var,alturas_alerta,redes,estaciones,series,series_areal,series_rast,observaciones,observaciones_areal,observaciones_rast,escenas,table_constraints to matviews;
grant select on observaciones_guardadas,observaciones_areal_guardadas,observaciones_rast_guardadas to matviews;

COMMIT;