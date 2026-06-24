BEGIN;

ALTER TABLE metadata_catalog DROP CONSTRAINT metadata_catalog_name_sourcetype_key;

ALTER TABLE metadata_catalog ADD CONSTRAINT metadata_catalog_name_key UNIQUE (name);

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
