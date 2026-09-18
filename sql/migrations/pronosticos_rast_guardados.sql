BEGIN;

CREATE TABLE public.pronosticos_rast_guardados (
    id integer NOT NULL,
    cor_id integer,
    series_id integer,
    timestart timestamp without time zone,
    timeend timestamp without time zone,
    qualifier character varying(50),
    valor raster not null
);

ALTER TABLE ONLY public.pronosticos_rast_guardados
    ADD CONSTRAINT pronosticos_rast_guardados_cor_id_series_id_ts_te_qu_key UNIQUE (cor_id, series_id, timestart, timeend, qualifier);

ALTER TABLE ONLY public.pronosticos_rast_guardados
    ADD CONSTRAINT pronosticos_rast_guardados_id_key UNIQUE (id);

ALTER TABLE ONLY public.pronosticos_rast_guardados
    ADD CONSTRAINT pronosticos_rast_guardados_cor_id_fkey FOREIGN KEY (cor_id) REFERENCES public.corridas_guardadas(id) ON DELETE CASCADE;

ALTER TABLE ONLY public.pronosticos_rast_guardados
    ADD CONSTRAINT pronosticos_rast_guardados_series_id_fkey FOREIGN KEY (series_id) REFERENCES public.series_rast(id);

COMMIT;