CREATE VIEW series_rast_prono_date_range AS
    SELECT
        series_id,
        cor_id,
        min(begin_date) begin_date,
        max(end_date) end_date,
        sum(count) count
    FROM series_rast_prono_date_range_by_qualifier
    GROUP BY series_id, cor_id
    ORDER BY series_id, cor_id;
