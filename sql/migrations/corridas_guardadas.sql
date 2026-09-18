BEGIN;
--
-- Name: corridas_guardadas; Type: TABLE; Schema: public; Owner: jbianchi
--

CREATE TABLE public.corridas_guardadas (
    cal_id integer NOT NULL,
    date timestamp without time zone NOT NULL,
    id integer NOT NULL,
    series_n integer,
    plan_cor_id integer
);

--
-- Name: pronosticos_guardados; Type: TABLE; Schema: public; Owner: jbianchi
--

CREATE TABLE public.pronosticos_guardados (
    id integer NOT NULL,
    cor_id integer,
    series_id integer,
    timestart timestamp without time zone,
    timeend timestamp without time zone,
    qualifier character varying(50)
);

--
-- Name: valores_prono_num_guardados; Type: TABLE; Schema: public; Owner: jbianchi
--

CREATE TABLE public.valores_prono_num_guardados (
    prono_id integer NOT NULL,
    valor real
);

--
-- Name: corridas_guardadas_cal_id_date_key; Type: CONSTRAINT; Schema: public; Owner: jbianchi
--

ALTER TABLE ONLY public.corridas_guardadas
    ADD CONSTRAINT corridas_guardadas_cal_id_date_key UNIQUE (cal_id, date);


--
-- Name: corridas_guardadas_id_key; Type: CONSTRAINT; Schema: public; Owner: jbianchi
--

ALTER TABLE ONLY public.corridas_guardadas
    ADD CONSTRAINT corridas_guardadas_id_key UNIQUE (id);


--
-- Name: pronosticos_guardados_cor_id_series_id_timestart_timeend_qu_key; Type: CONSTRAINT; Schema: public; Owner: jbianchi
--

ALTER TABLE ONLY public.pronosticos_guardados
    ADD CONSTRAINT pronosticos_guardados_cor_id_series_id_timestart_timeend_qu_key UNIQUE (cor_id, series_id, timestart, timeend, qualifier);


--
-- Name: pronosticos_guardados_id_key; Type: CONSTRAINT; Schema: public; Owner: jbianchi
--

ALTER TABLE ONLY public.pronosticos_guardados
    ADD CONSTRAINT pronosticos_guardados_id_key UNIQUE (id);


--
-- Name: valores_prono_num_guardados_prono_id_key; Type: CONSTRAINT; Schema: public; Owner: jbianchi
--

ALTER TABLE ONLY public.valores_prono_num_guardados
    ADD CONSTRAINT valores_prono_num_guardados_prono_id_key UNIQUE (prono_id);


--
-- Name: corridas_guardadas_cal_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: jbianchi
--

ALTER TABLE ONLY public.corridas_guardadas
    ADD CONSTRAINT corridas_guardadas_cal_id_fkey FOREIGN KEY (cal_id) REFERENCES public.calibrados(id);


--
-- Name: corridas_guardadas_plan_cor_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: jbianchi
--

ALTER TABLE ONLY public.corridas_guardadas
    ADD CONSTRAINT corridas_guardadas_plan_cor_id_fkey FOREIGN KEY (plan_cor_id) REFERENCES public.planes_corridas(id);


--
-- Name: pronosticos_guardados_cor_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: jbianchi
--

ALTER TABLE ONLY public.pronosticos_guardados
    ADD CONSTRAINT pronosticos_guardados_cor_id_fkey FOREIGN KEY (cor_id) REFERENCES public.corridas_guardadas(id) ON DELETE CASCADE;


--
-- Name: pronosticos_guardados_series_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: jbianchi
--

ALTER TABLE ONLY public.pronosticos_guardados
    ADD CONSTRAINT pronosticos_guardados_series_id_fkey FOREIGN KEY (series_id) REFERENCES public.series(id);


--
-- Name: valores_prono_num_guardados_prono_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: jbianchi
--

ALTER TABLE ONLY public.valores_prono_num_guardados
    ADD CONSTRAINT valores_prono_num_guardados_prono_id_fkey FOREIGN KEY (prono_id) REFERENCES public.pronosticos_guardados(id) ON DELETE CASCADE;

GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE public.corridas_guardadas TO usuario;

GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE public.pronosticos_guardados TO usuario;

GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE public.valores_prono_num_guardados TO usuario;

COMMIT;