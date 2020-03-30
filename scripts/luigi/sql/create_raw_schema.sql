create schema if not exists raw;

drop table if exists raw.etl_execution;

create table raw.etl_execution (
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

comment on table raw.etl_ejecucion is 'Metadata from ETL-RAW';
