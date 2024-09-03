CREATE FUNCTION get_series_table(tipo varchar) RETURNS varchar
AS $$
BEGIN
    RETURN CASE 
        WHEN tipo = 'areal'
            THEN 'series_areal'
        WHEN tipo = 'raster'
            THEN 'series_raster'
        ELSE
            'series'
        END;
END
$$
LANGUAGE plpgsql;
