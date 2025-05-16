BEGIN;

CREATE TABLE public.umbrales_territoriales (
    series_id integer NOT NULL,
    nombre character varying,
    valor real NOT NULL,
    estado character varying(1) NOT NULL
);

ALTER TABLE ONLY public.umbrales_territoriales
    ADD CONSTRAINT umbrales_territoriales_series_id_estado_key UNIQUE (series_id, estado);

ALTER TABLE ONLY public.umbrales_territoriales
    ADD CONSTRAINT umbrales_territoriales_series_id_valor_key UNIQUE (series_id, valor);


ALTER TABLE ONLY public.umbrales_territoriales
    ADD CONSTRAINT umbrales_territoriales_series_id_fkey FOREIGN KEY (series_id) REFERENCES public.series(id) ON DELETE CASCADE ON UPDATE CASCADE;

COMMIT;