create schema if not exists raw;

drop table if exists raw.etl_ejecucion;

create table raw.etl_ejecucion (
  "name" TEXT,
  "extention" TEXT,
  "schema" TEXT,
  "action" TEXT,
  "creator" TEXT,
  "machine" TEXT,
  "ip" TEXT,
  "creation_date" TEXT,
  "size" INTEGER,
  "location" TEXT,
  "entries" TEXT,
  "variables" TEXT,
  "script" TEXT,
  "log_script" TEXT,
  "status" TEXT
);

comment on table raw.etl_ejecucion is 'Metadatos de ejecucion del etl RAW';
