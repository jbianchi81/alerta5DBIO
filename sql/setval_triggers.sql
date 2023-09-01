--- series

CREATE OR REPLACE FUNCTION series_id_seq_setval()
RETURNS trigger AS $$
BEGIN
    PERFORM setval('series_id_seq',max_id.id) FROM (SELECT max(id) id FROM series) AS max_id;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;


CREATE TRIGGER series_id_seq_setval_trigger
    AFTER INSERT OR UPDATE OF id OR DELETE
    ON series
    FOR EACH STATEMENT
    EXECUTE PROCEDURE series_id_seq_setval();

--- estaciones

CREATE OR REPLACE FUNCTION estacion_unid_seq_setval()
RETURNS trigger AS $$
BEGIN
    PERFORM setval('estaciones_unid_seq',max_id.unid) FROM (SELECT max(unid) unid FROM estaciones) AS max_id;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;


CREATE TRIGGER estacion_unid_seq_setval_trigger
    AFTER INSERT OR UPDATE OF unid OR DELETE
    ON estaciones
    FOR EACH STATEMENT
    EXECUTE PROCEDURE estacion_unid_seq_setval();

--- areas_pluvio

CREATE OR REPLACE FUNCTION areas_pluvio_unid_seq_setval()
RETURNS trigger AS $$
BEGIN
    PERFORM setval('areas_pluvio_unid_seq',max_id.unid) FROM (SELECT max(unid) unid FROM areas_pluvio) AS max_id;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;


CREATE TRIGGER areas_pluvio_unid_seq_setval_trigger
    AFTER INSERT OR UPDATE OF unid OR DELETE
    ON areas_pluvio
    FOR EACH STATEMENT
    EXECUTE PROCEDURE areas_pluvio_unid_seq_setval();

--- var

CREATE OR REPLACE FUNCTION var_id_seq_setval()
RETURNS trigger AS $$
BEGIN
    PERFORM setval('var_id_seq',max_id.id) FROM (SELECT max(id) id FROM var) AS max_id;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;


CREATE TRIGGER var_id_seq_setval_trigger
    AFTER INSERT OR UPDATE OF id OR DELETE
    ON var
    FOR EACH STATEMENT
    EXECUTE PROCEDURE var_id_seq_setval();

--- procedimiento

CREATE OR REPLACE FUNCTION procedimiento_id_seq_setval()
RETURNS trigger AS $$
BEGIN
    PERFORM setval('procedimiento_id_seq',max_id.id) FROM (SELECT max(id) id FROM procedimiento) AS max_id;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;


CREATE TRIGGER procedimiento_id_seq_setval_trigger
    AFTER INSERT OR UPDATE OF id OR DELETE
    ON procedimiento
    FOR EACH STATEMENT
    EXECUTE PROCEDURE procedimiento_id_seq_setval();

--- unidades

CREATE OR REPLACE FUNCTION unidades_id_seq_setval()
RETURNS trigger AS $$
BEGIN
    PERFORM setval('unidades_id_seq',max_id.id) FROM (SELECT max(id) id FROM unidades) AS max_id;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;


CREATE TRIGGER unidades_id_seq_setval_trigger
    AFTER INSERT OR UPDATE OF id OR DELETE
    ON unidades
    FOR EACH STATEMENT
    EXECUTE PROCEDURE unidades_id_seq_setval();

--- fuentes

CREATE OR REPLACE FUNCTION fuentes_id_seq_setval()
RETURNS trigger AS $$
BEGIN
    PERFORM setval('fuentes_id_seq',max_id.id) FROM (SELECT max(id) id FROM fuentes) AS max_id;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;


CREATE TRIGGER fuentes_id_seq_setval_trigger
    AFTER INSERT OR UPDATE OF id OR DELETE
    ON fuentes
    FOR EACH STATEMENT
    EXECUTE PROCEDURE fuentes_id_seq_setval();

--- redes

CREATE OR REPLACE FUNCTION redes_id_seq_setval()
RETURNS trigger AS $$
BEGIN
    PERFORM setval('redes_id_seq',max_id.id) FROM (SELECT max(id) id FROM redes) AS max_id;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;


CREATE TRIGGER redes_id_seq_setval_trigger
    AFTER INSERT OR UPDATE OF id OR DELETE
    ON redes
    FOR EACH STATEMENT
    EXECUTE PROCEDURE redes_id_seq_setval();

-- escenas

CREATE OR REPLACE FUNCTION escenas_id_seq_setval()
RETURNS trigger AS $$
BEGIN
    PERFORM setval('escenas_id_seq',max_id.id) FROM (SELECT max(id) id FROM escenas) AS max_id;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;


CREATE TRIGGER escenas_id_seq_setval_trigger
    AFTER INSERT OR UPDATE OF id OR DELETE
    ON escenas
    FOR EACH STATEMENT
    EXECUTE PROCEDURE escenas_id_seq_setval();

-- modelos

CREATE OR REPLACE FUNCTION modelos_id_seq_setval()
RETURNS trigger AS $$
BEGIN
    PERFORM setval('modelos_id_seq',max_id.id) FROM (SELECT max(id) id FROM modelos) AS max_id;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;


CREATE TRIGGER modelos_id_seq_setval_trigger
    AFTER INSERT OR UPDATE OF id OR DELETE
    ON modelos
    FOR EACH STATEMENT
    EXECUTE PROCEDURE modelos_id_seq_setval();

-- calibrados

CREATE OR REPLACE FUNCTION calibrados_id_seq_setval()
RETURNS trigger AS $$
BEGIN
    PERFORM setval('calibrados_id_seq',max_id.id) FROM (SELECT max(id) id FROM calibrados) AS max_id;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;


CREATE TRIGGER calibrados_id_seq_setval_trigger
    AFTER INSERT OR UPDATE OF id OR DELETE
    ON calibrados
    FOR EACH STATEMENT
    EXECUTE PROCEDURE calibrados_id_seq_setval();

-- corridas

CREATE OR REPLACE FUNCTION corridas_id_seq_setval()
RETURNS trigger AS $$
BEGIN
    PERFORM setval('corridas_id_seq',max_id.id) FROM (SELECT max(id) id FROM corridas) AS max_id;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;


CREATE TRIGGER corridas_id_seq_setval_trigger
    AFTER INSERT OR UPDATE OF id OR DELETE
    ON corridas
    FOR EACH STATEMENT
    EXECUTE PROCEDURE corridas_id_seq_setval();

