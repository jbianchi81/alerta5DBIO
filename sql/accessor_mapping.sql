BEGIN;

CREATE TABLE public."VariableName" (
    "VariableName" character varying NOT NULL,
    href character varying
);

ALTER TABLE ONLY public."VariableName"
    ADD CONSTRAINT "VariableName_pkey" PRIMARY KEY ("VariableName");

grant select,update,delete,insert on "VariableName" to actualiza;

INSERT INTO "VariableName" ("VariableName") select distinct("VariableName") from var where "VariableName" is not null;

alter table var add constraint var_variablename_foreign_key foreign key ("VariableName") references "VariableName" ("VariableName") on delete set null;

drop table if exists accessor_feature_of_interest;
create table accessor_feature_of_interest (
    accessor_id varchar not null references accessors(name) on delete cascade,
    feature_id varchar not null,
    name varchar,
    geometry geometry,
    result jsonb,
    estacion_id integer references estaciones(unid) on delete set null,
    area_id integer references areas_pluvio(unid)  on delete set null,
    escena_id integer references escenas(id)  on delete set null,
    network_id varchar references redes(tabla_id)  on delete set null,
    primary key(accessor_id,feature_id)
);

drop table if exists accessor_observed_property;
create table accessor_observed_property (
    accessor_id varchar not null references accessors(name) on delete cascade,
    observed_property_id varchar not null,
    name varchar,
    result jsonb,
    variable_name varchar references "VariableName"("VariableName") on delete set null,
    primary key(accessor_id, observed_property_id)
);

drop table if exists accessor_unit_of_measurement;
create table accessor_unit_of_measurement (
    accessor_id varchar not null references accessors(name) on delete cascade,
    unit_of_measurement_id varchar not null,
    unit_id integer references unidades(id) on delete set null,
    primary key(accessor_id,unit_of_measurement_id)
);


drop table if exists accessor_timeseries_observation; 
create table accessor_timeseries_observation (
    accessor_id varchar not null references accessors(name) on delete cascade,
    timeseries_id varchar not null,
    result jsonb,
    series_puntual_id integer references series(id) on delete set null,
    series_areal_id integer references series_areal(id) on delete set null,
    series_rast_id integer references series_rast(id) on delete set null,
    feature_of_interest_id varchar,
    observed_property_id varchar,
    unit_of_measurement_id varchar,
    time_support interval,
    data_type varchar,
    primary key(accessor_id,timeseries_id), 
    foreign key(accessor_id, feature_of_interest_id) references accessor_feature_of_interest(accessor_id, feature_id),
    foreign key(accessor_id, observed_property_id) references accessor_observed_property(accessor_id, observed_property_id),
    foreign key(accessor_id, unit_of_measurement_id) references accessor_unit_of_measurement(accessor_id, unit_of_measurement_id),
    begin_position timestamp,
    end_position timestamp
);

drop table if exists accessor_time_value_pair;
create table accessor_time_value_pair (
    accessor_id varchar not null,
    timeseries_id varchar not null,
    timestamp timestamp not null,
    numeric_value real,
    json_value jsonb,
    raster_value raster,
    result jsonb,
    observaciones_puntual_id integer references observaciones(id) on delete set null,
    observaciones_areal_id integer references observaciones_areal(id)  on delete set null,
    observaciones_rast_id integer references observaciones_rast(id)  on delete set null,
    foreign key (accessor_id,timeseries_id) references accessor_timeseries_observation(accessor_id,timeseries_id) on delete cascade,
    primary key(accessor_id,timeseries_id,timestamp)
);

grant select,update,delete,insert on accessor_feature_of_interest to actualiza;
grant select,update,delete,insert on accessor_observed_property to actualiza;
grant select,update,delete,insert on accessor_timeseries_observation to actualiza;
grant select,update,delete,insert on accessor_time_value_pair to actualiza;
grant select,update,delete,insert on accessor_unit_of_measurement to actualiza;


create or replace view series_union_all as select id,'puntual' tipo,estacion_id, var_id, unit_id, proc_id, null fuentes_id from series union all select id,'areal' tipo, area_id estacion_id, var_id,proc_id,unit_id,fuentes_id from series_areal union all select id,'raster' tipo, escena_id estacion_id, var_id, proc_id, unit_id, fuentes_id from series_rast;

grant select on series_union_all to actualiza;


--
-- PostgreSQL database dump
--

--
-- Name: datatypes; Type: TABLE; Schema: public; Owner: 
--

CREATE TABLE IF NOT EXISTS public.datatypes (
    id integer primary key NOT NULL,
    term character varying UNIQUE NOT NULL,
    in_waterml1_cv boolean DEFAULT false,
    waterml2_code character varying,
    waterml2_uri character varying
);


CREATE SEQUENCE IF NOT EXISTS public.datatypes_id_seq
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
-- Name: datatypes_id_seq; Type: SEQUENCE SET; Schema: public; Owner:
--

SELECT pg_catalog.setval('public.datatypes_id_seq', 49, true);


--
-- Name: TABLE datatypes; Type: ACL; Schema: public; Owner: 
--

GRANT SELECT ON TABLE public.datatypes TO actualiza;
GRANT SELECT ON TABLE public.datatypes TO sololectura;


alter table var drop constraint var_datatype_fkey, add constraint var_datatype_fkey foreign key (datatype) references datatypes(term);

--
-- PostgreSQL database dump complete
--


COMMIT;