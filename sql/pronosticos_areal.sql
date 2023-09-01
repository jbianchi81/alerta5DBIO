--
-- PostgreSQL database dump
--

-- Dumped from database version 14.8 (Ubuntu 14.8-0ubuntu0.22.04.1)
-- Dumped by pg_dump version 14.8 (Ubuntu 14.8-0ubuntu0.22.04.1)

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: pronosticos; Type: TABLE; Schema: public; Owner: leyden
--

CREATE TABLE public.pronosticos_areal (
    id integer NOT NULL,
    cor_id integer NOT NULL,
    series_id integer NOT NULL,
    timestart timestamp without time zone NOT NULL,
    timeend timestamp without time zone NOT NULL,
    qualifier character varying(50) DEFAULT 'main'::character varying,
    valor real NOT NULL
);


--
-- Name: pronosticos_id_seq; Type: SEQUENCE; Schema: public; Owner: leyden
--

CREATE SEQUENCE public.pronosticos_areal_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: pronosticos_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: leyden
--

ALTER SEQUENCE public.pronosticos_areal_id_seq OWNED BY public.pronosticos_areal.id;


--
-- Name: pronosticos id; Type: DEFAULT; Schema: public; Owner: leyden
--

ALTER TABLE ONLY public.pronosticos_areal ALTER COLUMN id SET DEFAULT nextval('public.pronosticos_areal_id_seq'::regclass);


--
-- Name: pronosticos pronosticos_cor_id_series_id_timestart_timeend_qualifier_key; Type: CONSTRAINT; Schema: public; Owner: leyden
--

ALTER TABLE ONLY public.pronosticos_areal
    ADD CONSTRAINT pronosticos_areal_cor_id_series_id_timestart_timeend_qualifier_key UNIQUE (cor_id, series_id, timestart, timeend, qualifier);


--
-- Name: pronosticos pronosticos_id_key; Type: CONSTRAINT; Schema: public; Owner: leyden
--

ALTER TABLE ONLY public.pronosticos_areal
    ADD CONSTRAINT pronosticos_area_id_key UNIQUE (id);


--
-- Name: pronosticos pronosticos_cor_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: leyden
--

ALTER TABLE ONLY public.pronosticos_areal
    ADD CONSTRAINT pronosticos_areal_cor_id_fkey FOREIGN KEY (cor_id) REFERENCES public.corridas(id) ON DELETE CASCADE;


--
-- Name: pronosticos pronosticos_series_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: leyden
--

ALTER TABLE ONLY public.pronosticos_areal
    ADD CONSTRAINT pronosticos_areal_series_id_fkey FOREIGN KEY (series_id) REFERENCES public.series_areal(id) ON DELETE CASCADE;


--
-- Name: TABLE pronosticos; Type: ACL; Schema: public; Owner: leyden
--

GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE public.pronosticos_areal TO actualiza;


--
-- Name: SEQUENCE pronosticos_id_seq; Type: ACL; Schema: public; Owner: leyden
--

GRANT ALL ON SEQUENCE public.pronosticos_areal_id_seq TO actualiza;


--
-- PostgreSQL database dump complete
--

