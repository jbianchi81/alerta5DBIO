begin;
alter table observaciones add column validada boolean default false;
alter table observaciones_areal add column validada boolean default false;
alter table observaciones_rast add column validada boolean default false;
commit;