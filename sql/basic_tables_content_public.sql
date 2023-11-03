-- pg_dump -d meteorology -t unidades -t var -t procedimiento  -t tipo_estaciones -a > basic_tables_content.sql

--
-- PostgreSQL database dump
--

-- Dumped from database version 9.5.24
-- Dumped by pg_dump version 12.6 (Ubuntu 12.6-0ubuntu0.20.04.1)

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

\set ON_ERROR_STOP on

--
-- Data for Name: procedimiento; Type: TABLE DATA; Schema: public; Owner: alerta5
--

COPY public.procedimiento (id, nombre, abrev, descripcion) FROM stdin;
1	medición directa	medicion	Medición directa
2	Curva de gasto	curva	Obtenido a partir de curva de gasto
3	Interpolado	interp	Interpolado linealmente a partir de datos observados en la vecindad espaciotemporal
5	Estimado	est	Estimado a partir de observaciones indirectas
4	Simulado	sim	Simulado mediante un modelo
6	Análisis	anal	Análisis a partir de datos observados
7	Climatología	clim	Promedios climáticos
8	Traza	traza	Traza pronosticada
9	Derivado	deriv	Derivado de datos observados
\.


--
-- Data for Name: unidades; Type: TABLE DATA; Schema: public; Owner: alerta5
--

COPY public.unidades (id, nombre, abrev, "UnitsID", "UnitsType") FROM stdin;
14	adimensional	-	0	Unknown
21	metros,metros cúbicos por segundo	m,m^3/s	0	Unknown
9	milímetros	mm	54	Length
11	metros	m	52	Length
19	centímetros	cm	47	Length
20	kilómetros	km	51	Length
10	metros cúbicos por segundo	m^3/s	36	Flow
12	grados centígrados	ºC	96	Temperature
13	kilómetros por hora	km/h	116	Velocity
15	porcentaje	%	1	Proportion
16	grados	º	2	Angle
17	hectoPascales	hP	315	Pressure
18	miliBares	mBar	90	Pressure
22	milímetros por día	mm/d	305	velocity
23	contenido volumétrico	v/v	350	Proportion
24	desvíos estándar * 1000	sd*1000	351	deviation
0	Unknown		0	Unknown
144	MegaJoules por metro cuadrado	MJ/m^2	144	Energy per Area
353	Okta	Okta	353	Proportion
312	Hectómetro cúbico	(hm)^3	312	Volume
33	Watts por metro cuadrado	W/m^2	33	Energy Flux
104	day	d	104	Time
102	minute	min	102	Time
103	hour	h	103	Time
356	kilocalorías por centímetro cuadrado por día	kcal/cm2/dia	0	EnergyFlux
357	milímetros de mercurio	mmHg	86	Pressure/Stress
355	metros por segundo	m/s	119	Velocity
106	month	mon	106	Time
358	décimos	décimos	0	Proportion
\.

--
-- Data for Name: datatypes; Type: TABLE DATA; Schema: public; Owner:
--

INSERT INTO public.datatypes (id, term, in_waterml1_cv, waterml2_code, waterml2_uri) VALUES (2, 'Sporadic', true, NULL, NULL) ON CONFLICT (id) DO NOTHING;
INSERT INTO public.datatypes (id, term, in_waterml1_cv, waterml2_code, waterml2_uri) VALUES (3, 'Cumulative', true, NULL, NULL) ON CONFLICT (id) DO NOTHING;
INSERT INTO public.datatypes (id, term, in_waterml1_cv, waterml2_code, waterml2_uri) VALUES (4, 'Incremental', true, NULL, NULL) ON CONFLICT (id) DO NOTHING;
INSERT INTO public.datatypes (id, term, in_waterml1_cv, waterml2_code, waterml2_uri) VALUES (8, 'Constant Over Interval', true, NULL, NULL) ON CONFLICT (id) DO NOTHING;
INSERT INTO public.datatypes (id, term, in_waterml1_cv, waterml2_code, waterml2_uri) VALUES (9, 'Categorical', true, NULL, NULL) ON CONFLICT (id) DO NOTHING;
INSERT INTO public.datatypes (id, term, in_waterml1_cv, waterml2_code, waterml2_uri) VALUES (1, 'Continuous', true, 'Continuous', 'http://www.opengis.net/def/timeseries/InterpolationCode/Continuous') ON CONFLICT (id) DO NOTHING;
INSERT INTO public.datatypes (id, term, in_waterml1_cv, waterml2_code, waterml2_uri) VALUES (36, 'Average in Preceding Interval', false, 'Average preceding', 'http://www.opengis.net/def/timeseries/InterpolationCode/AveragePrec') ON CONFLICT (id) DO NOTHING;
INSERT INTO public.datatypes (id, term, in_waterml1_cv, waterml2_code, waterml2_uri) VALUES (37, 'Average in Succeeding Interval', false, 'Average Succeeding', 'http://www.opengis.net/def/timeseries/InterpolationCode/AverageSucc') ON CONFLICT (id) DO NOTHING;
INSERT INTO public.datatypes (id, term, in_waterml1_cv, waterml2_code, waterml2_uri) VALUES (38, 'Constant in Preceding Interval', false, 'Constant Preceding', 'http://www.opengis.net/def/timeseries/InterpolationCode/ConstPrec') ON CONFLICT (id) DO NOTHING;
INSERT INTO public.datatypes (id, term, in_waterml1_cv, waterml2_code, waterml2_uri) VALUES (39, 'Constant in Succeeding Interval', false, 'Constant Succeeding', 'http://www.opengis.net/def/timeseries/InterpolationCode/ConstSucc') ON CONFLICT (id) DO NOTHING;
INSERT INTO public.datatypes (id, term, in_waterml1_cv, waterml2_code, waterml2_uri) VALUES (40, 'Discontinuous', false, 'Discontinuous', 'http://www.opengis.net/def/timeseries/InterpolationCode/Discontinuous') ON CONFLICT (id) DO NOTHING;
INSERT INTO public.datatypes (id, term, in_waterml1_cv, waterml2_code, waterml2_uri) VALUES (41, 'Instantaneous Total', false, 'Instant Total', 'http://www.opengis.net/def/timeseries/InterpolationCode/InstantTotal') ON CONFLICT (id) DO NOTHING;
INSERT INTO public.datatypes (id, term, in_waterml1_cv, waterml2_code, waterml2_uri) VALUES (42, 'Maximum in Preceding Interval', false, 'Maximum Preceding', 'http://www.opengis.net/def/timeseries/InterpolationCode/MaxPrec') ON CONFLICT (id) DO NOTHING;
INSERT INTO public.datatypes (id, term, in_waterml1_cv, waterml2_code, waterml2_uri) VALUES (43, 'Maximum in Succeeding Interval', false, 'Maximum Succeeding', 'http://www.opengis.net/def/timeseries/InterpolationCode/MaxSucc') ON CONFLICT (id) DO NOTHING;
INSERT INTO public.datatypes (id, term, in_waterml1_cv, waterml2_code, waterml2_uri) VALUES (44, 'Minimum in Preceding Interval', false, 'Minimum Preceding', 'http://www.opengis.net/def/timeseries/InterpolationCode/MinPrec') ON CONFLICT (id) DO NOTHING;
INSERT INTO public.datatypes (id, term, in_waterml1_cv, waterml2_code, waterml2_uri) VALUES (45, 'Minimum in Succeeding Interval', false, 'Minimum Succeeding', 'http://www.opengis.net/def/timeseries/InterpolationCode/MinSucc') ON CONFLICT (id) DO NOTHING;
INSERT INTO public.datatypes (id, term, in_waterml1_cv, waterml2_code, waterml2_uri) VALUES (5, 'Average', true, 'Average Succeeding', 'http://www.opengis.net/def/timeseries/InterpolationCode/AverageSucc') ON CONFLICT (id) DO NOTHING;
INSERT INTO public.datatypes (id, term, in_waterml1_cv, waterml2_code, waterml2_uri) VALUES (6, 'Maximum', true, 'Maximum Succeeding', 'http://www.opengis.net/def/timeseries/InterpolationCode/MaximumSucc') ON CONFLICT (id) DO NOTHING;
INSERT INTO public.datatypes (id, term, in_waterml1_cv, waterml2_code, waterml2_uri) VALUES (7, 'Minimum', true, 'Minimum Succeeding', 'http://www.opengis.net/def/timeseries/InterpolationCode/MinimumSucc') ON CONFLICT (id) DO NOTHING;
INSERT INTO public.datatypes (id, term, in_waterml1_cv, waterml2_code, waterml2_uri) VALUES (46, 'Preceding Total', false, NULL, NULL) ON CONFLICT (id) DO NOTHING;
INSERT INTO public.datatypes (id, term, in_waterml1_cv, waterml2_code, waterml2_uri) VALUES (47, 'Succeeding Total', false, NULL, NULL) ON CONFLICT (id) DO NOTHING;
INSERT INTO public.datatypes (id, term, in_waterml1_cv, waterml2_code, waterml2_uri) VALUES (48, 'Mode in Preceding Interval', false, NULL, NULL) ON CONFLICT (id) DO NOTHING;
INSERT INTO public.datatypes (id, term, in_waterml1_cv, waterml2_code, waterml2_uri) VALUES (49, 'Mode in Succeeding Interval', false, NULL, NULL) ON CONFLICT (id) DO NOTHING;

--
-- Data for Name: var; Type: TABLE DATA; Schema: public; Owner: alerta5
--

COPY public.var (id, var, nombre, abrev, type, datatype, valuetype, "GeneralCategory", "VariableName", "SampleMedium", arr_names, def_unit_id, "timeSupport", def_hora_corte) FROM stdin;
67	Hms	Altura hidrométrica media semanal	alturamediasemana	num	Average	Field Observation	Hydrology	Gage height	Surface Water	\N	11	7 days	00:00:00
42	visib	visibilidad	visib	num	Continuous	Field Observation	Meteorology	Visibility	Air	\N	15	00:00:00	\N
70	QMaxIn	QMax Instantáneo	QMaxIn	num	Maximum	Field Observation	Hydrology	Discharge	Unknown	\N	10	00:00:00	\N
71	QMinIn	QMin Instantáneo	QMinIn	num	Minimum	Field Observation	Hydrology	Discharge	Surface Water	\N	10	00:00:00	\N
72	Hmax	Humedad relativa máxima diaria	Hmax	num	Maximum	Field Observation	Meteorology	Relative humidity	Air	\N	15	24:00:00	\N
73	TAgua	Temperatura del agua	TAgua	num	Continuous	Field Observation	Water Quality	Temperature	Surface Water	\N	12	00:00:00	\N
69	QMDmin	Caudal Medio Diario Mínimo	QMDmin	num	Minimum	Field Observation	Hydrology	Discharge	Surface Water	\N	10	1 year	\N
68	QMDmax	Caudal Medio Diario Máximo	QMDmax	num	Maximum	Field Observation	Hydrology	Discharge	Surface Water	\N	10	1 year	\N
74	gc	guía de crecidas	gc	num	Cumulative	Model Simulation Result	Hydrology	flood guidance	Precipitation	\N	9	24:00:00	\N
50	Hmmax	Altura hidrométrica máxima mensual	alturamaxmes	num	Maximum	Field Observation	Hydrology	Gage height	Surface Water	\N	11	720:00:00	00:00:00
14	SRad	Radiación solar	solarrad	num	Average	Field Observation	Climate	Global Radiation	Air	\N	144	00:00:00	\N
19	HQ	par Altura/Caudal	parHQ	numarr	Sporadic	Field Observation	Hydrology	\N	Surface Water	\N	21	00:00:00	\N
4	Q	Caudal	caudal	num	Continuous	Derived Value	Hydrology	Discharge	Surface Water	\N	10	00:00:00	\N
2	H	Altura hidrométrica	altura	num	Continuous	Field Observation	Hydrology	Gage height	Surface Water	\N	11	00:00:00	\N
39	Hmd	Altura hidrométrica media diaria	alturamediadia	num	Average	Field Observation	Hydrology	Gage height	Surface Water	\N	11	1 day	00:00:00
47	api	índice de precipitación antecedente	API	num	Average	Model Simulation Result	Hydrology	antecedent precipitation index	Soil	\N	14	7 days	\N
38	Pacum	precipicación acumulada	Pacum	num	Cumulative	Field Observation	Unknown	Precipitation	Unknown	\N	9	\N	\N
15	ETP	Evapotranspiración potencial	etp	num	Cumulative	Derived Value	Climate	Evapotranspiration	Air	\N	22	1 day	09:00:00
1	P	precipitación diaria 12Z	precip_diaria_met	num	Cumulative	Field Observation	Climate	Precipitation	Precipitation	\N	22	1 day	09:00:00
35	Htide	Altura de marea astronómica	altura_marea	num	Continuous	Model Simulation Result	Hydrology	Tidal stage	Surface Water	\N	11	00:00:00	\N
36	Hmeteo	Altura de marea meteorológica	altura_marea_meteo	num	Continuous	Model Simulation Result	Hydrology	Water level	Surface water	\N	11	00:00:00	\N
37	nieve	nivel de nieve	nieve	num	Continuous	Field Observation	Unknown	Snow depth	Unknown	\N	11	00:00:00	\N
43	Trocio	temperatura de rocío	Trocio	num	Continuous	Field Observation	Meteorology	Temperature, dew point	Atmosphere	\N	12	\N	\N
20	SM	Humedad del suelo	SM	num	Sporadic	Derived Value	Hydrology	Volumetric water content	Soil	\N	23	1 day	\N
21	FM	Magnitud de inundación	FM	num	Sporadic	Derived Value	Hydrology	Flood magnitude	Surface Water	\N	24	1 day	\N
9	Vmax	Velocidad del viento máxima	velvientomax	num	Maximum	Field Observation	Climate	Wind speed	Air	\N	13	1 day	00:00:00
45	Tbulbo	temperatura de bulbo húmedo	Tbulbo	num	Continuous	Field Observation	Climate	Temperature	Atmosphere	\N	12	\N	\N
46	PmesAn	anomalía de precipitación mensual	PmesAn	num	Cumulative	Field Observation	Climate	Precipitation	Precipitation	\N	9	744:00:00	\N
48	Qmm	caudal medio mensual	caudalmediomes	num	Average	Field Observation	Hydrology	Gage height	Surface Water	\N	11	1 mon	00:00:00
49	Hmmin	Altura hidrométrica mínima mensual	alturaminmes	num	Average	Field Observation	Hydrology	Gage height	Surface Water	\N	11	1 mon	00:00:00
10	Vmed	Velocidad del viento media	velvientomedia	num	Minimum	Field Observation	Climate	Wind speed	Air	\N	13	1 day	00:00:00
17	nubdia	nubosidad media diaria	nubdia	num	Average	Field Observation	Climate	Cloud cover	Air	\N	353	1 day	00:00:00
13	Hel	Heliofanía	helio	num	Cumulative	Field Observation	Climate	Sunshine duration	Air	\N	103	1 day	00:00:00
30	WE	Area Saturada	areasat	numarr	Continuous	Derived Value	Hydrology	Water extent	Surface Water	{sat,"no sat",nubes}	14	00:00:00	\N
32	Hgeo	Altura geométrica	Hgeo	num	Continuous	Field Observation	Unknown	\N	Unknown	\N	0	00:00:00	\N
28	a:H	Aforos:Altura	altura	num	Sporadic	Field Observation	Hydrology	Gage height	Surface Water	\N	11	00:00:00	\N
64	Vdh	Dirección del viento modal horaria	dirvientohora	num	Continuous	Field Observation	Climate	Wind Direction	Air	\N	16	01:00:00	00:00:00
51	Qmmin	Caudal mínimo mensual	caudalminmes	num	Average	Field Observation	Hydrology	Discharge	Surface Water	\N	10	1 mon	00:00:00
29	a:Q	Aforos:Caudal	caudal	num	Sporadic	Field Observation	Hydrology	Discharge	Surface Water	\N	10	00:00:00	\N
52	Qmmax	Caudal máximo mensual	caudalmaxmes	num	Average	Field Observation	Hydrology	Discharge	Surface Water	\N	10	1 mon	00:00:00
33	Hmm	Altura hidrométrica media mensual	alturamediames	num	Average	Field Observation	Hydrology	Gage height	Surface Water	\N	11	1 mon	00:00:00
53	T	Temperatura	temp	num	Continuous	Field Observation	Climate	Temperature	Air	\N	12	\N	\N
55	Vv	Velocidad del viento	velviento	num	Continuous	Field Observation	Climate	Wind Speed	Air	\N	13	\N	\N
57	Vd	Dirección del viento	dirviento	num	Continuous	Field Observation	Climate	Wind Direction	Air	\N	16	\N	\N
58	Hr	Humedad relativa	humrel	num	Continuous	Field Observation	Climate	Relative humidity	Air	\N	15	\N	\N
60	Pr	Presión barométrica	pres	num	Continuous	Field Observation	Climate	Barometric pressure	Air	\N	17	\N	\N
56	Vvh	Velocidad del viento horaria	velvientohora	num	Average	Field Observation	Climate	Wind Speed	Air	\N	13	01:00:00	00:00:00
62	nub	nubosidad	nub	num	Continuous	Field Observation	Climate	Cloud cover	Air	\N	353	\N	\N
63	pnm	presión al nivel del mar	pres_nm	num	Continuous	Field Observation	Climate	Sea-level pressure	Air	\N	17	\N	\N
27	Pi	Precipitación a intervalo nativo	precip_inst	num	Incremental	Field Observation	Climate	Precipitation	Precipitation	\N	9	\N	\N
40	Qmd	Caudal medio diario	caudalmediodia	num	Average	Field Observation	Hydrology	Discharge	Surface Water	\N	10	1 day	00:00:00
22	Qafl	Caudal Afluente	Qafluente	num	Continuous	Field Observation	Hydrology	Reservoir inflow	Surface Water	\N	10	1 day	00:00:00
25	Qtra	Caudal Transferido	Qtransfer	num	Continuous	Field Observation	Hydrology	Transfered discharge	Surface Water	\N	10	1 day	00:00:00
26	Vut	Volumen Útil	Vutil	num	Continuous	Field Observation	Hydrology	Reservoir storage	Surface Water	\N	312	1 day	00:00:00
41	Pmes	precipitación mensual	Pmensual	num	Cumulative	Field Observation	Climate	Precipitation	Precipitation	\N	9	1 mon	00:00:00
5	Tmin	Temperatura mínima	tempmin	num	Minimum	Field Observation	Climate	Temperature	Air	\N	12	1 day	00:00:00
7	Tmed	Temperatura media	tempmed	num	Average	Field Observation	Climate	Temperature	Air	\N	12	1 day	00:00:00
6	Tmax	Temperatura máxima	tempmax	num	Maximum	Field Observation	Climate	Temperature	Air	\N	12	1 day	00:00:00
8	Tsue	Temperatura del suelo	tempsuelo	num	Average	Field Observation	Climate	Temperature	Air	\N	12	1 day	00:00:00
18	pnmdia	presión al nivel del mar media diaria	pnmdia	num	Average	Field Observation	Climate	Sea-level pressure	Air	\N	17	1 day	00:00:00
23	Qefl	Caudal Efluente	Qefluente	num	Continuous	Field Observation	Hydrology	Reservoir outflow	Surface Water	\N	10	1 day	00:00:00
12	HR	Humedad relativa media diaria	humrel	num	Average	Field Observation	Climate	Relative humidity	Air	\N	15	1 day	00:00:00
16	Pmed	Presión barométrica media diaria	presionmedia	num	Average	Field Observation	Climate	Barometric pressure	Air	\N	17	1 day	00:00:00
24	Qver	Caudal Vertido	Qvertido	num	Continuous	Field Observation	Hydrology	Reservoir spilled	Surface Water	\N	10	1 day	00:00:00
11	Vdir	dirección del viento modal diaria	dirviento	num	Average	Field Observation	Climate	Wind direction	Air	\N	16	1 day	00:00:00
31	Ph	precipitación horaria	precip_horaria	num	Cumulative	Field Observation	Climate	Precipitation	Precipitation	\N	9	01:00:00	00:00:00
54	Th	Temperatura horaria	temp	num	Average	Field Observation	Climate	Temperature	Air	\N	12	01:00:00	00:00:00
34	P3h	precipitación 3 horaria	precip_3h	num	Cumulative	Field Observation	Climate	Precipitation	Precipitation	\N	9	03:00:00	00:00:00
59	Hrh	Humedad relativa media horaria	humrelhora	num	Average	Field Observation	Climate	Relative humidity	Air	\N	15	\N	00:00:00
61	Prh	Presión barométrica media horaria	preshora	num	Average	Field Observation	Climate	Barometric pressure	Air	\N	17	01:00:00	00:00:00
65	ugrd	Viento - componente u	ugrd	num	Continuous	Model Simulation Result	Meteorology	Wind speed	Air	\N	355	00:00:00	\N
66	vgrd	Viento - componente v	vgrd	num	Continuous	Model Simulation Result	Meteorology	Wind speed	Air	\N	355	00:00:00	\N
85	H1h	Altura hidrométrica horaria	alturahora	num	Continuous	Field Observation	Hydrology	Gage height	Surface Water	\N	11	01:00:00	\N
87	Q1h	Caudal horario	caudalhora	num	Continuous	Field Observation	Hydrology	Discharge	Surface Water	\N	10	01:00:00	\N
\.



--
-- Data for Name: tipo_estaciones; Type: TABLE DATA; Schema: public; Owner: alerta5
--

COPY public.tipo_estaciones (tipo, id, nombre) FROM stdin;
H	2	Hidrológica
M	1	Meteorológica
P	3	Pluviométrica
A	4	Combinada
E	5	Embalse
V	6	Virtual
\.

--
-- Name: procedimiento_id_seq; Type: SEQUENCE SET; Schema: public; Owner: alerta5
--

SELECT pg_catalog.setval('public.procedimiento_id_seq', 5, true);


--
-- Name: tipo_estaciones_id_seq; Type: SEQUENCE SET; Schema: public; Owner: alerta5
--

SELECT pg_catalog.setval('public.tipo_estaciones_id_seq', 3, true);


--
-- Name: unidades_id_seq; Type: SEQUENCE SET; Schema: public; Owner: alerta5
--

SELECT pg_catalog.setval('public.unidades_id_seq', 355, true);


--
-- Name: var_id_seq; Type: SEQUENCE SET; Schema: public; Owner: alerta5
--

SELECT pg_catalog.setval('public.var_id_seq', 48, true);

--
-- Name: escenas_id_seq; Type: SEQUENCE SET; Schema: public; Owner: alerta5
--

SELECT pg_catalog.setval('public.escenas_id_seq', 21, true);

--
-- PostgreSQL database dump complete
--

REFRESH MATERIALIZED VIEW public.series_date_range;

REFRESH MATERIALIZED VIEW public.series_areal_date_range;

REFRESH MATERIALIZED VIEW public.series_rast_date_range;

REFRESH MATERIALIZED VIEW public.series_json;

REFRESH MATERIALIZED VIEW public.series_areal_json_no_geom;