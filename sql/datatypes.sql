--
-- PostgreSQL database dump
--

-- Dumped from database version 9.5.25
-- Dumped by pg_dump version 14.7 (Ubuntu 14.7-0ubuntu0.22.04.1)

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

--
-- Name: datatypes; Type: TABLE; Schema: public; Owner: 
--

CREATE TABLE public.datatypes (
    id integer NOT NULL,
    term character varying NOT NULL,
    in_waterml1_cv boolean DEFAULT false,
    waterml2_code character varying,
    waterml2_uri character varying
);


CREATE SEQUENCE public.datatypes_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;




--
-- Name: datatypes_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: 
--

ALTER SEQUENCE public.datatypes_id_seq OWNED BY public.datatypes.id;


--
-- Name: datatypes id; Type: DEFAULT; Schema: public; Owner: 
--

ALTER TABLE ONLY public.datatypes ALTER COLUMN id SET DEFAULT nextval('public.datatypes_id_seq'::regclass);


--
-- Data for Name: datatypes; Type: TABLE DATA; Schema: public; Owner:
--

COPY public.datatypes (id, term, in_waterml1_cv, waterml2_code, waterml2_uri) FROM stdin;
2	Sporadic	t	\N	\N
3	Cumulative	t	\N	\N
4	Incremental	t	\N	\N
8	Constant Over Interval	t	\N	\N
9	Categorical	t	\N	\N
1	Continuous	t	Continuous	http://www.opengis.net/def/timeseries/InterpolationCode/Continuous
36	Average in Preceding Interval	f	Average preceding	http://www.opengis.net/def/timeseries/InterpolationCode/AveragePrec
37	Average in Succeeding Interval	f	Average Succeeding	http://www.opengis.net/def/timeseries/InterpolationCode/AverageSucc
38	Constant in Preceding Interval	f	Constant Preceding	http://www.opengis.net/def/timeseries/InterpolationCode/ConstPrec
39	Constant in Succeeding Interval	f	Constant Succeeding	http://www.opengis.net/def/timeseries/InterpolationCode/ConstSucc
40	Discontinuous	f	Discontinuous	http://www.opengis.net/def/timeseries/InterpolationCode/Discontinuous
41	Instantaneous Total	f	Instant Total	http://www.opengis.net/def/timeseries/InterpolationCode/InstantTotal
42	Maximum in Preceding Interval	f	Maximum Preceding	http://www.opengis.net/def/timeseries/InterpolationCode/MaxPrec
43	Maximum in Succeeding Interval	f	Maximum Succeeding	http://www.opengis.net/def/timeseries/InterpolationCode/MaxSucc
44	Minimum in Preceding Interval	f	Minimum Preceding	http://www.opengis.net/def/timeseries/InterpolationCode/MinPrec
45	Minimum in Succeeding Interval	f	Minimum Succeeding	http://www.opengis.net/def/timeseries/InterpolationCode/MinSucc
5	Average	t	Average Succeeding	http://www.opengis.net/def/timeseries/InterpolationCode/AverageSucc
6	Maximum	t	Maximum Succeeding	http://www.opengis.net/def/timeseries/InterpolationCode/MaximumSucc
7	Minimum	t	Minimum Succeeding	http://www.opengis.net/def/timeseries/InterpolationCode/MinimumSucc
46	Preceding Total	f	\N	\N
47	Succeeding Total	f	\N	\N
48	Mode in Preceding Interval	f	\N	\N
49	Mode in Succeeding Interval	f	\N	\N
\.


--
-- Name: datatypes_id_seq; Type: SEQUENCE SET; Schema: public; Owner:
--

SELECT pg_catalog.setval('public.datatypes_id_seq', 49, true);


--
-- Name: datatypes datatypes_pkey; Type: CONSTRAINT; Schema: public; Owner: 
--

ALTER TABLE ONLY public.datatypes
    ADD CONSTRAINT datatypes_pkey PRIMARY KEY (id);


--
-- Name: datatypes datatypes_term_key; Type: CONSTRAINT; Schema: public; Owner: 
--

ALTER TABLE ONLY public.datatypes
    ADD CONSTRAINT datatypes_term_key UNIQUE (term);


--
-- Name: TABLE datatypes; Type: ACL; Schema: public; Owner: 
--

GRANT SELECT ON TABLE public.datatypes TO actualiza;
GRANT SELECT ON TABLE public.datatypes TO sololectura;


alter table var add constraint var_datatype_fk foreign key (datatype) references datatypes(term);

--
-- PostgreSQL database dump complete
--

