create schema if not exists prediction;

drop table if exists prediction.ut_execution;

create table prediction.ut_execution (
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
  "param_bucket" TEXT,
  "model_name" TEXT,
  "prediction_date" TEXT,
  "uuid" TEXT
);

comment on table cleaned.ut_execution is 'Metadata from Unit Test - Predictions';
