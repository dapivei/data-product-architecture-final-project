create schema if not exists mlpreproc;

drop table if exists mlpreproc.ut_execution;

create table mlpreproc.ut_execution (
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
  "param_bucket" TEXT
);

comment on table mlpreproc.ut_execution is 'Metadata from Unit Test - MLPreproc';
