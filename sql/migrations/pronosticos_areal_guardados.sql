BEGIN;

--
-- Name: pronosticos_areal_guardados; Type: TABLE; Schema: public; Owner: jbianchi
--

CREATE TABLE public.pronosticos_areal_guardados (
    id integer NOT NULL,
    cor_id integer,
    series_id integer,
    timestart timestamp without time zone,
    timeend timestamp without time zone,
    qualifier character varying(50),
    valor real not null
);


ALTER TABLE ONLY public.pronosticos_areal_guardados
    ADD CONSTRAINT pronosticos_areal_guardados_cor_id_series_id_ts_te_qu_key UNIQUE (cor_id, series_id, timestart, timeend, qualifier);

ALTER TABLE ONLY public.pronosticos_areal_guardados
    ADD CONSTRAINT pronosticos_areal_guardados_id_key UNIQUE (id);

ALTER TABLE ONLY public.pronosticos_areal_guardados
    ADD CONSTRAINT pronosticos_areal_guardados_cor_id_fkey FOREIGN KEY (cor_id) REFERENCES public.corridas_guardadas(id) ON DELETE CASCADE;

ALTER TABLE ONLY public.pronosticos_areal_guardados
    ADD CONSTRAINT pronosticos_areal_guardados_series_id_fkey FOREIGN KEY (series_id) REFERENCES public.series_areal(id);

COMMIT;