create schema if not exists biasfairness;

drop table if exists biasfairness.aeq_metadata;

create table biasfairness.aeq_metadata (
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

comment on table biasfairness.aeq_metadata is 'Metadata from Bias & Fairness metrics';
