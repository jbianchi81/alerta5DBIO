ALTER TABLE series_mon_stats
ADD COLUMN valores real[],
ADD COLUMN percentage_complete real,
ADD COLUMN p13 real,
ADD COLUMN p28 real,
ADD COLUMN p72 real,
ADD COLUMN p87 real;