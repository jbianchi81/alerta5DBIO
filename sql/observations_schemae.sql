-- pg_dump meteorology -t redes -t estaciones -t areas_pluvio_unid_seq -t areas_pluvio -t var -t series -t series_areal -t observaciones -t observaciones_areal -t valores_num -t valores_numarr -t valores_num_areal -t valores_numarr_areal -t procedimiento -t tipo_estaciones -t fuentes -t unidades -t accessor_types -t accessors -t series_date_range -t alturas_alerta -t escenas -t series_rast -t observaciones_rast -t series_rast_date_range -t series_areal_date_range -t asociaciones -t asociaciones_view -s -C -O -x > observations_schemae.sql

--
-- PostgreSQL database dump
--

-- Dumped from database version 9.5.24
-- Dumped by pg_dump version 9.5.24

-- SET statement_timeout = 0;
-- SET lock_timeout = 0;
-- SET client_encoding = 'UTF8';
-- SET standard_conforming_strings = on;
-- SELECT pg_catalog.set_config('search_path', '', false);
-- SET check_function_bodies = false;
-- SET xmloption = content;
-- SET client_min_messages = warning;
-- SET row_security = off;

-- SET default_tablespace = '';

-- SET default_with_oids = false;

\set ON_ERROR_STOP on

--
-- Name: accessors; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.accessors (
    class character varying,
    url character varying,
    series_tipo character varying,
    series_source_id integer,
    time_update timestamp without time zone,
    name character varying NOT NULL,
    config json,
    series_id integer,
    upload_fields json DEFAULT '{}'::json,
    title character varying,
    token varchar,
    token_expiry_date timestamp
);


--
-- Name: areas_pluvio_unid_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.areas_pluvio_unid_seq
    START WITH 240
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: areas_pluvio; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.areas_pluvio (
    id integer,
    geom public.geometry(Polygon,4326),
    exutorio public.geometry(Point,4326),
    nombre character varying(64),
    area double precision DEFAULT 0,
    unid integer DEFAULT nextval('public.areas_pluvio_unid_seq'::regclass) NOT NULL,
    rho real DEFAULT 0.5,
    ae real DEFAULT 1,
    wp real DEFAULT 0.03,
    uru_index integer,
    activar boolean DEFAULT true,
    as_max real,
    rast public.raster,
    mostrar boolean DEFAULT true,
    exutorio_id integer
);

--
-- Name: redes; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.redes (
    tabla_id character varying,
    id integer NOT NULL,
    nombre character varying,
    public boolean DEFAULT true,
    public_his_plata boolean DEFAULT false NOT NULL
);

--
-- Name: redes_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.redes_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: redes_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.redes_id_seq OWNED BY public.redes.id;

--
-- Name: id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.redes ALTER COLUMN id SET DEFAULT nextval('public.redes_id_seq'::regclass);

--
-- Name: redes_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.redes
    ADD CONSTRAINT redes_id_key UNIQUE (id);


--
-- Name: redes_tabla_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.redes
    ADD CONSTRAINT redes_tabla_id_key UNIQUE (tabla_id);

--
-- Name: estaciones; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.estaciones (
    tabla character varying references redes(tabla_id) NOT NULL,
    id integer NOT NULL,
    tipo character varying(1),
    "real" boolean,
    nombre character varying,
    id_externo character varying,
    has_obs boolean,
    has_area boolean,
    has_prono boolean,
    rio character varying,
    distrito character varying,
    pais character varying,
    geom public.geometry(Point),
    var character varying,
    cero_ign real,
    cero_mop real,
    id_cuenca integer,
    abrev character varying,
    rule real[],
    propietario character varying,
    unid integer NOT NULL,
    automatica boolean DEFAULT false,
    url character varying,
    habilitar boolean DEFAULT true,
    coordinates_url integer[],
    ubicacion character varying,
    localidad character varying,
    tipo_2 character varying,
    orden integer DEFAULT 100,
    observaciones character varying,
    altitud real
);


--
-- Name: observaciones; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.observaciones (
    id bigint NOT NULL,
    series_id integer,
    timestart timestamp without time zone,
    timeend timestamp without time zone,
    nombre character varying,
    descripcion character varying,
    unit_id integer,
    timeupdate timestamp without time zone DEFAULT now()
);


--
-- Name: series; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.series (
    id integer NOT NULL,
    estacion_id integer NOT NULL,
    var_id integer NOT NULL,
    proc_id integer NOT NULL,
    unit_id integer NOT NULL
);


--
-- Name: valores_numarr; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.valores_numarr (
    obs_id bigint NOT NULL,
    valor real[]
);


--
-- Name: valores_num; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.valores_num (
    obs_id bigint NOT NULL,
    valor real
);


--
-- Name: observaciones_areal; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.observaciones_areal (
    id integer NOT NULL,
    series_id integer NOT NULL,
    timestart timestamp without time zone,
    timeend timestamp without time zone,
    nombre character varying,
    descripcion character varying,
    unit_id integer,
    timeupdate timestamp without time zone DEFAULT now()
);


--
-- Name: series_areal; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.series_areal (
    id integer NOT NULL,
    area_id integer NOT NULL,
    proc_id integer NOT NULL,
    var_id integer NOT NULL,
    unit_id integer NOT NULL,
    fuentes_id integer NOT NULL
);


--
-- Name: valores_num_areal; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.valores_num_areal (
    obs_id integer NOT NULL,
    valor real NOT NULL
);


--
-- Name: var; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.var (
    id integer NOT NULL,
    var character varying(6) NOT NULL,
    nombre character varying,
    abrev character varying,
    type character varying DEFAULT 'num'::character varying,
    datatype character varying DEFAULT 'Continuous'::character varying,
    valuetype character varying DEFAULT 'Field Observation'::character varying,
    "GeneralCategory" character varying DEFAULT 'Unknown'::character varying NOT NULL,
    "VariableName" character varying,
    "SampleMedium" character varying DEFAULT 'Unknown'::character varying NOT NULL,
    arr_names text[],
    def_unit_id integer DEFAULT 0 NOT NULL,
    "timeSupport" interval,
    def_hora_corte interval
);


--
-- Name: estaciones_unid_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.estaciones_unid_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: estaciones_unid_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.estaciones_unid_seq OWNED BY public.estaciones.unid;



--
-- Name: fuentes; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.fuentes (
    id integer NOT NULL,
    nombre character varying NOT NULL,
    data_table character varying,
    data_column character varying,
    tipo character varying,
    def_proc_id integer,
    def_dt interval DEFAULT '1 day'::interval,
    hora_corte interval hour DEFAULT '12:00:00'::interval hour,
    def_unit_id integer,
    def_var_id integer,
    fd_column character varying,
    mad_table character varying,
    scale_factor real,
    data_offset real,
    def_pixel_height real,
    def_pixel_width real,
    def_srid integer DEFAULT 4326,
    def_extent public.geometry DEFAULT public.st_setsrid(public.st_makepolygon(public.st_geomfromtext('LINESTRING(-70 -40, -70 -10, -40 -10, -40 -40, -70 -40)'::text)), 4326),
    date_column character varying,
    def_pixeltype character varying(5) DEFAULT '32BF'::character varying,
    abstract character varying,
    source character varying,
    public boolean DEFAULT true
);


--
-- Name: procedimiento; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.procedimiento (
    id integer NOT NULL,
    nombre character varying NOT NULL,
    abrev character varying,
    descripcion character varying
);


--
-- Name: unidades; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.unidades (
    id integer NOT NULL,
    nombre character varying,
    abrev character varying,
    "UnitsID" integer DEFAULT 0 NOT NULL,
    "UnitsType" character varying DEFAULT 'Unknown'::character varying NOT NULL
);


--
-- Name: valores_numarr_areal; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.valores_numarr_areal (
    obs_id integer NOT NULL,
    valor real[] NOT NULL
);


--
-- Name: fuentes_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.fuentes_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: fuentes_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.fuentes_id_seq OWNED BY public.fuentes.id;


--
-- Name: observaciones_areal_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.observaciones_areal_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: observaciones_areal_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.observaciones_areal_id_seq OWNED BY public.observaciones_areal.id;


--
-- Name: observaciones_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.observaciones_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: observaciones_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.observaciones_id_seq OWNED BY public.observaciones.id;


--
-- Name: procedimiento_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.procedimiento_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: procedimiento_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.procedimiento_id_seq OWNED BY public.procedimiento.id;




--
-- Name: series_areal_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.series_areal_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: series_areal_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.series_areal_id_seq OWNED BY public.series_areal.id;


--
-- Name: series_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.series_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: series_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.series_id_seq OWNED BY public.series.id;


--
-- Name: tipo_estaciones; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.tipo_estaciones (
    tipo character varying(1),
    id integer NOT NULL,
    nombre character varying
);


--
-- Name: tipo_estaciones_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.tipo_estaciones_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: tipo_estaciones_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.tipo_estaciones_id_seq OWNED BY public.tipo_estaciones.id;


--
-- Name: unidades_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.unidades_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: unidades_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.unidades_id_seq OWNED BY public.unidades.id;


--
-- Name: var_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.var_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: var_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.var_id_seq OWNED BY public.var.id;


--
-- Name: unid; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.estaciones ALTER COLUMN unid SET DEFAULT nextval('public.estaciones_unid_seq'::regclass);


--
-- Name: id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.fuentes ALTER COLUMN id SET DEFAULT nextval('public.fuentes_id_seq'::regclass);


--
-- Name: id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.observaciones ALTER COLUMN id SET DEFAULT nextval('public.observaciones_id_seq'::regclass);


--
-- Name: id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.observaciones_areal ALTER COLUMN id SET DEFAULT nextval('public.observaciones_areal_id_seq'::regclass);


--
-- Name: id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.procedimiento ALTER COLUMN id SET DEFAULT nextval('public.procedimiento_id_seq'::regclass);




--
-- Name: id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.series ALTER COLUMN id SET DEFAULT nextval('public.series_id_seq'::regclass);


--
-- Name: id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.series_areal ALTER COLUMN id SET DEFAULT nextval('public.series_areal_id_seq'::regclass);


--
-- Name: id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.tipo_estaciones ALTER COLUMN id SET DEFAULT nextval('public.tipo_estaciones_id_seq'::regclass);


--
-- Name: id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.unidades ALTER COLUMN id SET DEFAULT nextval('public.unidades_id_seq'::regclass);


--
-- Name: id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.var ALTER COLUMN id SET DEFAULT nextval('public.var_id_seq'::regclass);


--
-- Name: accessors_name_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.accessors
    ADD CONSTRAINT accessors_name_key UNIQUE (name);


--
-- Name: areas_pluvio_unid_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.areas_pluvio
    ADD CONSTRAINT areas_pluvio_unid_key UNIQUE (unid);


--
-- Name: estaciones_id_externo_tabla_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.estaciones
    ADD CONSTRAINT estaciones_id_externo_tabla_key UNIQUE (id_externo, tabla);


--
-- Name: estaciones_id_tabla_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.estaciones
    ADD CONSTRAINT estaciones_id_tabla_key UNIQUE (id, tabla);


--
-- Name: estaciones_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.estaciones
    ADD CONSTRAINT estaciones_pkey PRIMARY KEY (unid);


--
-- Name: fuentes_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.fuentes
    ADD CONSTRAINT fuentes_pkey PRIMARY KEY (id);


--
-- Name: observaciones_areal_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.observaciones_areal
    ADD CONSTRAINT observaciones_areal_pkey PRIMARY KEY (id);


--
-- Name: observaciones_areal_series_id_timestart_timeend_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.observaciones_areal
    ADD CONSTRAINT observaciones_areal_series_id_timestart_timeend_key UNIQUE (series_id, timestart, timeend);


--
-- Name: observaciones_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.observaciones
    ADD CONSTRAINT observaciones_pkey PRIMARY KEY (id);


--
-- Name: observaciones_series_id_timestart_timeend_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.observaciones
    ADD CONSTRAINT observaciones_series_id_timestart_timeend_key UNIQUE (series_id, timestart, timeend);


--
-- Name: procedimiento_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.procedimiento
    ADD CONSTRAINT procedimiento_pkey PRIMARY KEY (id);


--
-- Name: series_areal_fuentes_id_proc_id_unit_id_var_id_area_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.series_areal
    ADD CONSTRAINT series_areal_fuentes_id_proc_id_unit_id_var_id_area_id_key UNIQUE (fuentes_id, proc_id, unit_id, var_id, area_id);


--
-- Name: series_areal_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.series_areal
    ADD CONSTRAINT series_areal_id_key UNIQUE (id);


--
-- Name: series_estacion_id_var_id_proc_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.series
    ADD CONSTRAINT series_estacion_id_var_id_proc_id_key UNIQUE (estacion_id, var_id, proc_id);


--
-- Name: series_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.series
    ADD CONSTRAINT series_pkey PRIMARY KEY (id);


--
-- Name: tipo_estaciones_tipo_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.tipo_estaciones
    ADD CONSTRAINT tipo_estaciones_tipo_key UNIQUE (tipo);


--
-- Name: unidades_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.unidades
    ADD CONSTRAINT unidades_pkey PRIMARY KEY (id);


--
-- Name: valores_num_areal_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.valores_num_areal
    ADD CONSTRAINT valores_num_areal_pkey PRIMARY KEY (obs_id);


--
-- Name: valores_num_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.valores_num
    ADD CONSTRAINT valores_num_pkey PRIMARY KEY (obs_id);


--
-- Name: valores_numarr_areal_obs_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.valores_numarr_areal
    ADD CONSTRAINT valores_numarr_areal_obs_id_key UNIQUE (obs_id);


--
-- Name: valores_numarr_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.valores_numarr
    ADD CONSTRAINT valores_numarr_pkey PRIMARY KEY (obs_id);


--
-- Name: var_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.var
    ADD CONSTRAINT var_pkey PRIMARY KEY (id);


--
-- Name: var_var_GeneralCategory_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.var
    ADD CONSTRAINT "var_var_GeneralCategory_key" UNIQUE (var, "GeneralCategory");


--
-- Name: sidx_estaciones_geom; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX sidx_estaciones_geom ON public.estaciones USING gist (geom);


--
-- Name: area_pluvio_calc; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER area_pluvio_calc BEFORE INSERT ON public.areas_pluvio FOR EACH ROW EXECUTE PROCEDURE public.area_calc();


--
-- Name: estacion_id_trigger; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER estacion_id_trigger BEFORE INSERT OR UPDATE ON public.estaciones FOR EACH ROW EXECUTE PROCEDURE public.estacion_id_trigger();


--
-- Name: hora_corte_trig; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER hora_corte_trig BEFORE INSERT ON public.observaciones_areal FOR EACH ROW EXECUTE PROCEDURE public.obs_hora_corte_constraint_trigger();


--
-- Name: obs_dt_trig; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER obs_dt_trig BEFORE INSERT ON public.observaciones_areal FOR EACH ROW EXECUTE PROCEDURE public.obs_dt_constraint_trigger();


--
-- Name: obs_puntual_dt_constraint_trigger; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER obs_puntual_dt_constraint_trigger BEFORE INSERT OR UPDATE ON public.observaciones FOR EACH ROW EXECUTE PROCEDURE public.obs_puntual_dt_constraint_trigger();


--
-- Name: obs_range_tr; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER obs_range_tr BEFORE INSERT ON public.observaciones_areal FOR EACH ROW EXECUTE PROCEDURE public.obs_range_constraint_trigger();

ALTER TABLE public.observaciones_areal DISABLE TRIGGER obs_range_tr;


--
-- Name: areas_pluvio_exutorio_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.areas_pluvio
    ADD CONSTRAINT areas_pluvio_exutorio_id_fkey FOREIGN KEY (exutorio_id) REFERENCES public.estaciones(unid);


--
-- Name: estaciones_tabla_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

-- ALTER TABLE ONLY public.estaciones
--     ADD CONSTRAINT estaciones_tabla_fkey FOREIGN KEY (tabla) REFERENCES public.redes(tabla_id);


--
-- Name: fuentes_def_proc_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.fuentes
    ADD CONSTRAINT fuentes_def_proc_id_fkey FOREIGN KEY (def_proc_id) REFERENCES public.procedimiento(id);


--
-- Name: fuentes_def_srid_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.fuentes
    ADD CONSTRAINT fuentes_def_srid_fkey FOREIGN KEY (def_srid) REFERENCES public.spatial_ref_sys(srid);


--
-- Name: fuentes_def_unit_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.fuentes
    ADD CONSTRAINT fuentes_def_unit_id_fkey FOREIGN KEY (def_unit_id) REFERENCES public.unidades(id);


--
-- Name: fuentes_def_var_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.fuentes
    ADD CONSTRAINT fuentes_def_var_id_fkey FOREIGN KEY (def_var_id) REFERENCES public.var(id);


--
-- Name: observaciones_areal_series_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.observaciones_areal
    ADD CONSTRAINT observaciones_areal_series_id_fkey FOREIGN KEY (series_id) REFERENCES public.series_areal(id);


--
-- Name: observaciones_areal_unit_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.observaciones_areal
    ADD CONSTRAINT observaciones_areal_unit_id_fkey FOREIGN KEY (unit_id) REFERENCES public.unidades(id);


--
-- Name: observaciones_series_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.observaciones
    ADD CONSTRAINT observaciones_series_id_fkey FOREIGN KEY (series_id) REFERENCES public.series(id) ON DELETE CASCADE;


--
-- Name: observaciones_unit_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.observaciones
    ADD CONSTRAINT observaciones_unit_id_fkey FOREIGN KEY (unit_id) REFERENCES public.unidades(id);


--
-- Name: series_areal_area_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.series_areal
    ADD CONSTRAINT series_areal_area_id_fkey FOREIGN KEY (area_id) REFERENCES public.areas_pluvio(unid);


--
-- Name: series_areal_fuentes_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.series_areal
    ADD CONSTRAINT series_areal_fuentes_id_fkey FOREIGN KEY (fuentes_id) REFERENCES public.fuentes(id);


--
-- Name: series_areal_proc_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.series_areal
    ADD CONSTRAINT series_areal_proc_id_fkey FOREIGN KEY (proc_id) REFERENCES public.procedimiento(id);


--
-- Name: series_areal_unit_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.series_areal
    ADD CONSTRAINT series_areal_unit_id_fkey FOREIGN KEY (unit_id) REFERENCES public.unidades(id);


--
-- Name: series_areal_var_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.series_areal
    ADD CONSTRAINT series_areal_var_id_fkey FOREIGN KEY (var_id) REFERENCES public.var(id);


--
-- Name: series_estacion_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.series
    ADD CONSTRAINT series_estacion_id_fkey FOREIGN KEY (estacion_id) REFERENCES public.estaciones(unid);


--
-- Name: series_proc_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.series
    ADD CONSTRAINT series_proc_id_fkey FOREIGN KEY (proc_id) REFERENCES public.procedimiento(id);


--
-- Name: series_var_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.series
    ADD CONSTRAINT series_var_id_fkey FOREIGN KEY (var_id) REFERENCES public.var(id);


--
-- Name: tipo_est; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.estaciones
    ADD CONSTRAINT tipo_est FOREIGN KEY (tipo) REFERENCES public.tipo_estaciones(tipo);


--
-- Name: unid_es; Type: FK CONSTRAINT; Schema: public; Owner: -
--

-- ALTER TABLE ONLY public.areas_pluvio
--     ADD CONSTRAINT unid_es FOREIGN KEY (unid) REFERENCES public.estaciones(unid);


--
-- Name: unidades_ser; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.series
    ADD CONSTRAINT unidades_ser FOREIGN KEY (unit_id) REFERENCES public.unidades(id);


--
-- Name: valores_num_areal_obs_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.valores_num_areal
    ADD CONSTRAINT valores_num_areal_obs_id_fkey FOREIGN KEY (obs_id) REFERENCES public.observaciones_areal(id);


--
-- Name: valores_num_obs_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.valores_num
    ADD CONSTRAINT valores_num_obs_id_fkey FOREIGN KEY (obs_id) REFERENCES public.observaciones(id) ON DELETE CASCADE;


--
-- Name: valores_numarr_areal_obs_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.valores_numarr_areal
    ADD CONSTRAINT valores_numarr_areal_obs_id_fkey FOREIGN KEY (obs_id) REFERENCES public.observaciones_areal(id);


--
-- Name: valores_numarr_obs_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.valores_numarr
    ADD CONSTRAINT valores_numarr_obs_id_fkey FOREIGN KEY (obs_id) REFERENCES public.observaciones(id) ON DELETE CASCADE;


--
-- Name: var_def_unit_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.var
    ADD CONSTRAINT var_def_unit_id_fkey FOREIGN KEY (def_unit_id) REFERENCES public.unidades(id);


--
-- PostgreSQL database dump complete
--

CREATE MATERIALIZED VIEW public.series_date_range AS
 SELECT series.id AS series_id,
    min(observaciones.timestart) AS timestart,
    max(observaciones.timestart) AS timeend,
    count(observaciones.timestart) AS count
   FROM public.series,
    public.observaciones
  WHERE (series.id = observaciones.series_id)
  GROUP BY series.id
  ORDER BY series.id
  WITH NO DATA;

-- GRANT SELECT ON TABLE public.series_date_range TO actualiza;

REFRESH MATERIALIZED VIEW public.series_date_range;

CREATE TABLE public.alturas_alerta (
    unid integer NOT NULL,
    nombre character varying,
    valor real NOT NULL,
    estado character varying(1) NOT NULL
);

ALTER TABLE ONLY public.alturas_alerta
    ADD CONSTRAINT alturas_alerta_unid_estado_key UNIQUE (unid, estado);


--
-- Name: alturas_alerta alturas_alerta_unid_valor_key; Type: CONSTRAINT; Schema: public; Owner: alerta5
--

ALTER TABLE ONLY public.alturas_alerta
    ADD CONSTRAINT alturas_alerta_unid_valor_key UNIQUE (unid, valor);


--
-- Name: alturas_alerta alturas_alerta_unid_fkey; Type: FK CONSTRAINT; Schema: public; Owner: alerta5
--

ALTER TABLE ONLY public.alturas_alerta
    ADD CONSTRAINT alturas_alerta_unid_fkey FOREIGN KEY (unid) REFERENCES public.estaciones(unid);


--
-- Name: TABLE alturas_alerta; Type: ACL; Schema: public; Owner: alerta5
--

-- GRANT SELECT,INSERT,UPDATE ON TABLE public.alturas_alerta TO actualiza;

--
-- Name: escenas; Type: TABLE; Schema: public; Owner: alerta5
--

CREATE TABLE public.escenas (
    id integer NOT NULL,
    geom public.geometry NOT NULL,
    nombre character varying
);

--
-- Name: escenas_id_seq; Type: SEQUENCE; Schema: public; Owner: alerta5
--

CREATE SEQUENCE public.escenas_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


-- ALTER TABLE public.escenas_id_seq OWNER TO alerta5;

--
-- Name: escenas_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: alerta5
--

ALTER SEQUENCE public.escenas_id_seq OWNED BY public.escenas.id;


--
-- Name: escenas id; Type: DEFAULT; Schema: public; Owner: alerta5
--

ALTER TABLE ONLY public.escenas ALTER COLUMN id SET DEFAULT nextval('public.escenas_id_seq'::regclass);


--
-- Name: escenas escenas_pkey; Type: CONSTRAINT; Schema: public; Owner: alerta5
--

ALTER TABLE ONLY public.escenas
    ADD CONSTRAINT escenas_pkey PRIMARY KEY (id);


--
-- Name: TABLE escenas; Type: ACL; Schema: public; Owner: alerta5
--

-- GRANT ALL ON TABLE public.escenas TO actualiza;

--
-- Name: SEQUENCE escenas_id_seq; Type: ACL; Schema: public; Owner: alerta5
--

-- GRANT ALL ON SEQUENCE public.escenas_id_seq TO actualiza;

--
-- PostgreSQL database dump complete
--

CREATE TABLE public.observaciones_rast (
    id integer NOT NULL,
    series_id integer NOT NULL,
    timestart timestamp without time zone NOT NULL,
    timeend timestamp without time zone NOT NULL,
    valor public.raster NOT NULL,
    timeupdate timestamp without time zone DEFAULT now() NOT NULL
);


-- ALTER TABLE public.observaciones_rast OWNER TO alerta5;

--
-- Name: series_rast; Type: TABLE; Schema: public; Owner: alerta5
--

CREATE TABLE public.series_rast (
    id integer NOT NULL,
    escena_id integer NOT NULL,
    fuentes_id integer NOT NULL,
    var_id integer NOT NULL,
    proc_id integer NOT NULL,
    unit_id integer NOT NULL,
    nombre character varying
);


-- ALTER TABLE public.series_rast OWNER TO alerta5;

--
-- Name: observaciones_rast_id_seq; Type: SEQUENCE; Schema: public; Owner: alerta5
--

CREATE SEQUENCE public.observaciones_rast_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

--
-- Name: observaciones_rast_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: alerta5
--

ALTER SEQUENCE public.observaciones_rast_id_seq OWNED BY public.observaciones_rast.id;


--
-- Name: series_rast_id_seq; Type: SEQUENCE; Schema: public; Owner: alerta5
--

CREATE SEQUENCE public.series_rast_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

--
-- Name: series_rast_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: alerta5
--

ALTER SEQUENCE public.series_rast_id_seq OWNED BY public.series_rast.id;


--
-- Name: observaciones_rast id; Type: DEFAULT; Schema: public; Owner: alerta5
--

ALTER TABLE ONLY public.observaciones_rast ALTER COLUMN id SET DEFAULT nextval('public.observaciones_rast_id_seq'::regclass);


--
-- Name: series_rast id; Type: DEFAULT; Schema: public; Owner: alerta5
--

ALTER TABLE ONLY public.series_rast ALTER COLUMN id SET DEFAULT nextval('public.series_rast_id_seq'::regclass);


--
-- Name: observaciones_rast observaciones_rast_pkey; Type: CONSTRAINT; Schema: public; Owner: alerta5
--

ALTER TABLE ONLY public.observaciones_rast
    ADD CONSTRAINT observaciones_rast_pkey PRIMARY KEY (id);


--
-- Name: observaciones_rast observaciones_rast_series_id_timestart_timeend_key; Type: CONSTRAINT; Schema: public; Owner: alerta5
--

ALTER TABLE ONLY public.observaciones_rast
    ADD CONSTRAINT observaciones_rast_series_id_timestart_timeend_key UNIQUE (series_id, timestart, timeend);


--
-- Name: series_rast series_rast_escena_id_fuentes_id_var_id_proc_id_unit_id_key; Type: CONSTRAINT; Schema: public; Owner: alerta5
--

ALTER TABLE ONLY public.series_rast
    ADD CONSTRAINT series_rast_escena_id_fuentes_id_var_id_proc_id_unit_id_key UNIQUE (escena_id, fuentes_id, var_id, proc_id, unit_id);


--
-- Name: series_rast series_rast_pkey; Type: CONSTRAINT; Schema: public; Owner: alerta5
--

ALTER TABLE ONLY public.series_rast
    ADD CONSTRAINT series_rast_pkey PRIMARY KEY (id);


--
-- Name: observaciones_rast observaciones_rast_series_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: alerta5
--

ALTER TABLE ONLY public.observaciones_rast
    ADD CONSTRAINT observaciones_rast_series_id_fkey FOREIGN KEY (series_id) REFERENCES public.series_rast(id);


--
-- Name: series_rast series_rast_escena_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: alerta5
--

ALTER TABLE ONLY public.series_rast
    ADD CONSTRAINT series_rast_escena_id_fkey FOREIGN KEY (escena_id) REFERENCES public.escenas(id);


--
-- Name: series_rast series_rast_fuentes_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: alerta5
--

ALTER TABLE ONLY public.series_rast
    ADD CONSTRAINT series_rast_fuentes_id_fkey FOREIGN KEY (fuentes_id) REFERENCES public.fuentes(id);


--
-- Name: series_rast series_rast_proc_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: alerta5
--

ALTER TABLE ONLY public.series_rast
    ADD CONSTRAINT series_rast_proc_id_fkey FOREIGN KEY (proc_id) REFERENCES public.procedimiento(id);


--
-- Name: series_rast series_rast_unit_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: alerta5
--

ALTER TABLE ONLY public.series_rast
    ADD CONSTRAINT series_rast_unit_id_fkey FOREIGN KEY (unit_id) REFERENCES public.unidades(id);


--
-- Name: series_rast series_rast_var_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: alerta5
--

ALTER TABLE ONLY public.series_rast
    ADD CONSTRAINT series_rast_var_id_fkey FOREIGN KEY (var_id) REFERENCES public.var(id);


--
-- Name: TABLE observaciones_rast; Type: ACL; Schema: public; Owner: alerta5
--

-- GRANT ALL ON TABLE public.observaciones_rast TO actualiza;

--
-- Name: TABLE series_rast; Type: ACL; Schema: public; Owner: alerta5
--

-- GRANT ALL ON TABLE public.series_rast TO actualiza;

--
-- Name: SEQUENCE observaciones_rast_id_seq; Type: ACL; Schema: public; Owner: alerta5
--

-- GRANT USAGE ON SEQUENCE public.observaciones_rast_id_seq TO actualiza;

--
-- Name: SEQUENCE series_rast_id_seq; Type: ACL; Schema: public; Owner: alerta5
--

-- GRANT USAGE ON SEQUENCE public.series_rast_id_seq TO actualiza;

--
-- PostgreSQL database dump complete
--

--
-- Name: series_areal_date_range; Type: MATERIALIZED VIEW; Schema: public; Owner: -
--

CREATE MATERIALIZED VIEW public.series_areal_date_range AS
 SELECT series_areal.id AS series_id,
    min(observaciones_areal.timestart) AS timestart,
    max(observaciones_areal.timestart) AS timeend,
    count(observaciones_areal.timestart) AS count
   FROM public.series_areal,
    public.observaciones_areal
  WHERE (series_areal.id = observaciones_areal.series_id)
  GROUP BY series_areal.id
  ORDER BY series_areal.id
  WITH NO DATA;

REFRESH MATERIALIZED VIEW public.series_areal_date_range;

--
-- Name: series_rast_date_range; Type: MATERIALIZED VIEW; Schema: public; Owner: -
--

CREATE MATERIALIZED VIEW public.series_rast_date_range AS
 SELECT series_rast.id AS series_id,
    min(observaciones_rast.timestart) AS timestart,
    max(observaciones_rast.timestart) AS timeend,
    count(observaciones_rast.timestart) AS count
   FROM public.series_rast,
    public.observaciones_rast
  WHERE (series_rast.id = observaciones_rast.series_id)
  GROUP BY series_rast.id
  ORDER BY series_rast.id
  WITH NO DATA;

REFRESH MATERIALIZED VIEW public.series_rast_date_range;

CREATE TABLE public.asociaciones (
    id integer NOT NULL,
    source_tipo character varying,
    source_series_id integer NOT NULL,
    dest_tipo character varying,
    dest_series_id integer NOT NULL,
    agg_func character varying,
    dt interval,
    t_offset interval,
    "precision" integer,
    source_time_support interval,
    source_is_inst boolean,
    habilitar boolean DEFAULT true,
    expresion character varying
);


--
-- Name: asociaciones_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.asociaciones_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: asociaciones_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.asociaciones_id_seq OWNED BY public.asociaciones.id;


--
-- Name: asociaciones_view; Type: VIEW; Schema: public; Owner: -
--

CREATE VIEW public.asociaciones_view AS
 WITH s_all AS (
         SELECT 'puntual'::text AS tipo,
            series.id,
            estaciones.unid AS sitio_id,
            estaciones.tabla AS fuentes_id,
            series.var_id,
            series.proc_id,
            series.unit_id
           FROM public.estaciones,
            public.series
          WHERE (estaciones.unid = series.estacion_id)
        UNION ALL
         SELECT 'areal'::text AS tipo,
            series_areal.id,
            series_areal.area_id AS sitio_id,
            (series_areal.fuentes_id)::text AS fuentes_id,
            series_areal.var_id,
            series_areal.proc_id,
            series_areal.unit_id
           FROM public.series_areal
        UNION ALL
         SELECT 'raster'::text AS tipo,
            series_rast.id,
            series_rast.escena_id AS sitio_id,
            (series_rast.fuentes_id)::text AS fuentes_id,
            series_rast.var_id,
            series_rast.proc_id,
            series_rast.unit_id
           FROM public.series_rast
        )
 SELECT a.id,
    a.source_tipo,
    a.source_series_id,
    a.dest_tipo,
    a.dest_series_id,
    a.agg_func,
    (a.dt)::text AS dt,
    (COALESCE(a.t_offset, '00:00:00'::interval))::text AS t_offset,
    a."precision",
    (a.source_time_support)::text AS source_time_support,
    a.source_is_inst,
    s_source.sitio_id AS source_estacion_id,
    s_source.fuentes_id AS source_fuentes_id,
    s_source.var_id AS source_var_id,
    s_source.proc_id AS source_proc_id,
    s_source.unit_id AS source_unit_id,
    s_dest.sitio_id AS dest_estacion_id,
    s_dest.fuentes_id AS dest_fuentes_id,
    s_dest.var_id AS dest_var_id,
    s_dest.proc_id AS dest_proc_id,
    s_dest.unit_id AS dest_unit_id,
    a.habilitar,
    a.expresion
   FROM public.asociaciones a,
    s_all s_source,
    s_all s_dest
  WHERE (((a.source_tipo)::text = s_source.tipo) AND (a.source_series_id = s_source.id) AND ((a.dest_tipo)::text = s_dest.tipo) AND (a.dest_series_id = s_dest.id))
  ORDER BY a.id;


--
-- Name: asociaciones id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.asociaciones ALTER COLUMN id SET DEFAULT nextval('public.asociaciones_id_seq'::regclass);


--
-- Name: asociaciones asociaciones_dest_tipo_dest_series_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.asociaciones
    ADD CONSTRAINT asociaciones_dest_tipo_dest_series_id_key UNIQUE (dest_tipo, dest_series_id);


--
-- Name: asociaciones asociaciones_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.asociaciones
    ADD CONSTRAINT asociaciones_pkey PRIMARY KEY (id);


--
-- Name: asociaciones asociaciones_source_tipo_source_series_id_dest_tipo_dest_se_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.asociaciones
    ADD CONSTRAINT asociaciones_source_tipo_source_series_id_dest_tipo_dest_se_key UNIQUE (source_tipo, source_series_id, dest_tipo, dest_series_id, dt, t_offset, agg_func);


--
-- Name: asociaciones asociaciones_dest_series_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.asociaciones
    ADD CONSTRAINT asociaciones_dest_series_id_fkey FOREIGN KEY (dest_series_id) REFERENCES public.series(id);


--
-- Name: asociaciones asociaciones_source_series_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.asociaciones
    ADD CONSTRAINT asociaciones_source_series_id_fkey FOREIGN KEY (source_series_id) REFERENCES public.series(id);

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
-- Name: datatypes_id_seq; Type: SEQUENCE SET; Schema: public; Owner:
--

SELECT pg_catalog.setval('public.datatypes_id_seq', 49, true);


--
-- Name: TABLE datatypes; Type: ACL; Schema: public; Owner: 
--

-- GRANT SELECT ON TABLE public.datatypes TO actualiza;
-- GRANT SELECT ON TABLE public.datatypes TO sololectura;


-- alter table var drop constraint var_datatype_fkey;
alter table public.var add constraint var_datatype_fkey foreign key (datatype) references public.datatypes(term);

-- JSON VIEWS

CREATE MATERIALIZED VIEW public.series_json AS
SELECT
    series.id,
    json_build_object(
    'tipo','puntual',
    'id',series.id,
    'estacion',json_build_object(
        'id',estaciones.unid,
        'nombre',estaciones.nombre,
        'id_externo',estaciones.id_externo,
        'geom',ST_ASGEOJSON(estaciones.geom::public.geometry)::json,
        'tabla', estaciones.tabla,
        'pais', estaciones.pais,
        'rio', estaciones.rio,
        'has_obs', estaciones.has_obs,
        'tipo', estaciones.tipo,
        'automatica', estaciones.automatica,
        'habilitar', estaciones.habilitar,
        'propietario', estaciones.propietario,
        'abreviatura', estaciones.abrev,
        'localidad', estaciones.localidad,
        'real', estaciones.real,
        'nivel_alerta', nivel_alerta.valor,
        'nivel_evacuacion', nivel_evacuacion.valor,
        'nivel_aguas_bajas', nivel_aguas_bajas.valor,
        'altitud', estaciones.altitud,
        'public', redes.public,
        'cero_ign', estaciones.cero_ign,
        'red_id', redes.id,
        'red_nombre', redes.nombre
    ),
    'var', json_build_object(
        'id', var.id,
        'var', var.var,
        'nombre', var.nombre,
        'abrev', var.abrev,
        'type', var.type,
        'datatype', var.datatype,
        'valuetype', var.valuetype,
        'GeneralCategory', var."GeneralCategory",
        'VariableName', var."VariableName",
        'SampleMedium', var."SampleMedium",
        'def_unit_id', var.def_unit_id,
        'timeSupport', var."timeSupport",
        'def_hora_corte', var.def_hora_corte
    ),
        'procedimiento', json_build_object(
        'id', procedimiento.id,
        'nombre', procedimiento.nombre,
        'abrev', procedimiento.abrev,
        'descripcion', procedimiento.descripcion
    ),
    'unidades', json_build_object(
        'id', unidades.id,
        'nombre', unidades.nombre,
        'abrev', unidades.abrev,
        'UnitsID', unidades."UnitsID",
        'UnitsType', unidades."UnitsType"
    ),
    'fuente', json_build_object(),
    'date_range', json_build_object(
        'timestart', series_date_range.timestart,
        'timeend', series_date_range.timeend,
        'count', series_date_range.count
    )
)  AS serie
FROM public.series
JOIN  public.estaciones ON (public.series.estacion_id=public.estaciones.unid)
JOIN public.redes ON (public.estaciones.tabla=public.redes.tabla_id)
LEFT JOIN public.alturas_alerta  AS nivel_alerta ON (public.estaciones.unid=nivel_alerta.unid AND nivel_alerta.estado='a')
LEFT JOIN public.alturas_alerta AS nivel_evacuacion ON (public.estaciones.unid=nivel_evacuacion.unid AND nivel_evacuacion.estado='e')
LEFT JOIN  public.alturas_alerta  AS nivel_aguas_bajas ON (public.estaciones.unid=nivel_aguas_bajas.unid AND nivel_aguas_bajas.estado='b')
JOIN public.var ON (public.series.var_id = public.var.id)
JOIN public.procedimiento ON (public.series.proc_id = public.procedimiento.id)
JOIN public.unidades ON (public.series.unit_id = public.unidades.id)
LEFT JOIN public.series_date_range ON (public.series.id = public.series_date_range.series_id)
ORDER BY series.id
WITH NO DATA;

CREATE OR REPLACE VIEW public.table_constraints AS
SELECT 
    rel.relname AS table_name,
    con.conname AS constraint_name,
    con.contype AS constraint_type,
    array_agg(kcu.COLUMN_NAME::text) AS column_names
FROM pg_catalog.pg_constraint con
    INNER JOIN pg_catalog.pg_class rel ON rel.oid = con.conrelid
    INNER JOIN pg_catalog.pg_namespace nsp ON nsp.oid = connamespace
    INNER JOIN information_schema.key_column_usage kcu ON kcu.constraint_name = con.conname
    INNER JOIN public.fuentes ON rel.relname = public.fuentes.data_table
WHERE nsp.nspname = 'public' group by rel.relname, con.conname, con.contype;

CREATE MATERIALIZED VIEW series_areal_json AS
WITH table_constraints_json AS (
    SELECT
        table_constraints.table_name,
        json_build_object(
            'table_name', table_constraints.table_name,
            'constraints', array_agg(json_build_object(
                'constraint_name', table_constraints.constraint_name,
                'constraint_type', table_constraints.constraint_type
            ))
        ) AS constraints
    FROM table_constraints
    GROUP BY table_constraints.table_name )
SELECT
    series_areal.id,
    json_build_object(
    'tipo','areal',
    'id',series_areal.id,
    'estacion',json_build_object(
        'id',areas_pluvio.unid,
        'nombre',areas_pluvio.nombre,
        'geom',ST_ASGEOJSON(areas_pluvio.geom)::json,
        'exutorio',json_build_object(
            'id', estaciones.unid,
            'geom', ST_ASGEOJSON(estaciones.geom)::json,
            'tabla', estaciones.tabla
        )
    ),
    'var', json_build_object(
        'id', var.id,
        'var', var.var,
        'nombre', var.nombre,
        'abrev', var.abrev,
        'type', var.type,
        'datatype', var.datatype,
        'valuetype', var.valuetype,
        'GeneralCategory', var."GeneralCategory",
        'VariableName', var."VariableName",
        'SampleMedium', var."SampleMedium",
        'def_unit_id', var.def_unit_id,
        'timeSupport', var."timeSupport",
        'def_hora_corte', var.def_hora_corte
    ),
        'procedimiento', json_build_object(
        'id', procedimiento.id,
        'nombre', procedimiento.nombre,
        'abrev', procedimiento.abrev,
        'descripcion', procedimiento.descripcion
    ),
    'unidades', json_build_object(
        'id', unidades.id,
        'nombre', unidades.nombre,
        'abrev', unidades.abrev,
        'UnitsID', unidades."UnitsID",
        'UnitsType', unidades."UnitsType"
    ),
    'fuente', json_build_object(
        'id', fuentes.id,
        'nombre', fuentes.nombre,
        'data_table', fuentes.data_table,
        'data_column', fuentes.data_column,
        'tipo', fuentes.tipo,
        'def_proc_id', fuentes.def_proc_id,
        'def_dt', fuentes.def_dt,
        'hora_corte', fuentes.hora_corte,
        'def_unit_id', fuentes.def_unit_id,
        'def_var_id', fuentes.def_var_id,
        'fd_column', fuentes.fd_column,
        'mad_table', fuentes.mad_table,
        'scale_factor', fuentes.scale_factor,
        'data_offset', fuentes.data_offset,
        'def_extent', fuentes.def_extent,
        'date_column', fuentes.date_column,
        'def_pixeltype', fuentes.def_pixeltype,
        'abstract', fuentes.abstract,
        'source', fuentes.source,
        'public', fuentes.public,
        'constraints', table_constraints_json.constraints
    ),
    'date_range', json_build_object(
        'timestart', series_areal_date_range.timestart,
        'timeend', series_areal_date_range.timeend,
        'count', series_areal_date_range.count
    )
)  AS serie
FROM series_areal
JOIN areas_pluvio ON (series_areal.area_id = areas_pluvio.unid)
LEFT JOIN estaciones ON (areas_pluvio.exutorio_id = estaciones.unid)
JOIN var ON (series_areal.var_id = var.id)
JOIN procedimiento ON (series_areal.proc_id = procedimiento.id)
JOIN unidades ON (series_areal.unit_id = unidades.id)
JOIN fuentes ON (series_areal.fuentes_id = fuentes.id)
LEFT JOIN series_areal_date_range ON (series_areal.id = series_areal_date_range.series_id)
LEFT JOIN table_constraints_json ON (fuentes.data_table = table_constraints_json.table_name)
ORDER BY series_areal.id;

CREATE MATERIALIZED VIEW series_areal_json_no_geom AS
WITH table_constraints_json AS (
    SELECT
        table_constraints.table_name,
        json_build_object(
            'table_name', table_constraints.table_name,
            'constraints', array_agg(json_build_object(
                'constraint_name', table_constraints.constraint_name,
                'constraint_type', table_constraints.constraint_type
            ))
        ) AS constraints
    FROM table_constraints
    GROUP BY table_constraints.table_name )
SELECT
    series_areal.id,
    json_build_object(
    'tipo','areal',
    'id',series_areal.id,
    'estacion',json_build_object(
        'id',areas_pluvio.unid,
        'nombre',areas_pluvio.nombre,
        'exutorio',json_build_object(
            'id', estaciones.unid,
            'geom', ST_ASGEOJSON(estaciones.geom)::json,
            'tabla', estaciones.tabla
        )
    ),
    'var', json_build_object(
        'id', var.id,
        'var', var.var,
        'nombre', var.nombre,
        'abrev', var.abrev,
        'type', var.type,
        'datatype', var.datatype,
        'valuetype', var.valuetype,
        'GeneralCategory', var."GeneralCategory",
        'VariableName', var."VariableName",
        'SampleMedium', var."SampleMedium",
        'def_unit_id', var.def_unit_id,
        'timeSupport', var."timeSupport",
        'def_hora_corte', var.def_hora_corte
    ),
        'procedimiento', json_build_object(
        'id', procedimiento.id,
        'nombre', procedimiento.nombre,
        'abrev', procedimiento.abrev,
        'descripcion', procedimiento.descripcion
    ),
    'unidades', json_build_object(
        'id', unidades.id,
        'nombre', unidades.nombre,
        'abrev', unidades.abrev,
        'UnitsID', unidades."UnitsID",
        'UnitsType', unidades."UnitsType"
    ),
    'fuente', json_build_object(
        'id', fuentes.id,
        'nombre', fuentes.nombre,
        'data_table', fuentes.data_table,
        'data_column', fuentes.data_column,
        'tipo', fuentes.tipo,
        'def_proc_id', fuentes.def_proc_id,
        'def_dt', fuentes.def_dt,
        'hora_corte', fuentes.hora_corte,
        'def_unit_id', fuentes.def_unit_id,
        'def_var_id', fuentes.def_var_id,
        'fd_column', fuentes.fd_column,
        'mad_table', fuentes.mad_table,
        'scale_factor', fuentes.scale_factor,
        'data_offset', fuentes.data_offset,
        'def_extent', fuentes.def_extent,
        'date_column', fuentes.date_column,
        'def_pixeltype', fuentes.def_pixeltype,
        'abstract', fuentes.abstract,
        'source', fuentes.source,
        'public', fuentes.public,
        'constraints', table_constraints_json.constraints
    ),
    'date_range', json_build_object(
        'timestart', series_areal_date_range.timestart,
        'timeend', series_areal_date_range.timeend,
        'count', series_areal_date_range.count
    )
)  AS serie
FROM series_areal
JOIN areas_pluvio ON (series_areal.area_id = areas_pluvio.unid)
LEFT JOIN estaciones ON (areas_pluvio.exutorio_id = estaciones.unid)
JOIN var ON (series_areal.var_id = var.id)
JOIN procedimiento ON (series_areal.proc_id = procedimiento.id)
JOIN unidades ON (series_areal.unit_id = unidades.id)
JOIN fuentes ON (series_areal.fuentes_id = fuentes.id)
LEFT JOIN series_areal_date_range ON (series_areal.id = series_areal_date_range.series_id)
LEFT JOIN table_constraints_json ON (fuentes.data_table = table_constraints_json.table_name)
ORDER BY series_areal.id
WITH NO DATA;
