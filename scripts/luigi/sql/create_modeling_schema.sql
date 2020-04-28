create schema if not exists modeling;

drop table if exists modeling.etl_ejecucion;

create table modeling.etl_ejecucion (
  "model_name" TEXT,
  "model_type" TEXT,
  "max_depth" TEXT,
  "criterion" TEXT,
  "n_estimators" TEXT,
  "score_train" TEXT,
  "score_validation" TEXT
);

comment on table modeling.etl_ejecucion is 'Metadatos de ejecucion del
etl MODELING';
