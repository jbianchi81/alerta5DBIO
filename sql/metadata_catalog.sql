BEGIN;

CREATE TABLE public.metadata_catalog (
    gid integer NOT NULL,
    location character varying,
    sourcetype character varying,
    name character varying,
    title character varying,
    abstract character varying,
    description character varying,
    keywords character varying[],
    defaultcrs character varying,
    "xmlelement" xml,
    metadata_creation_date timestamp without time zone DEFAULT now(),
    boundingbox public.geometry,
    units character varying,
    frequency interval,
    resolution real,
    data_sources character varying[],
    services_provided character varying[]
);

CREATE SEQUENCE public.metadata_catalog_gid_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

ALTER TABLE ONLY public.metadata_catalog ALTER COLUMN gid SET DEFAULT nextval('public.metadata_catalog_gid_seq'::regclass);

ALTER TABLE ONLY public.metadata_catalog
    ADD CONSTRAINT metadata_catalog_name_sourcetype_key UNIQUE (name);


CREATE TABLE layer_fields (
    layer_name text NOT NULL REFERENCES metadata_catalog(name),
    field_name text NOT NULL,
    field_type text NOT NULL,
    min_occurs integer,
    max_occurs text,
    nillable boolean,
    documentation text,

    PRIMARY KEY (
        layer_name,
        field_name
    )
);


COMMIT;