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
  "size" TEXT,
  "location" TEXT,
  "status" TEXT,
  "param_year" TEXT,
  "param_month" TEXT,
  "param_day" TEXT,
  "param_bucket" TEXT
);

comment on table raw.etl_execution is 'Metadata from ETL-RAW';
