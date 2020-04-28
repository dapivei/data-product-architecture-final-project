create schema if not exists modeling;

drop table if exists modeling.ejecucion;

create table modeling.ejecucion (
  "model_name" TEXT,
  "model_type" TEXT,
  "schema" TEXT,
  "action" TEXT,
  "creator" TEXT,
  "machine" TEXT,
  "ip" TEXT,
  "date" TEXT,
  "location" TEXT,
  "status" TEXT,
  "max_depth" TEXT,
  "criterion" TEXT,
  "n_estimators" TEXT,
  "score_train" TEXT);

comment on table modeling.ejecucion is 'Metadatos de ejecucion del
etl MODELING';
