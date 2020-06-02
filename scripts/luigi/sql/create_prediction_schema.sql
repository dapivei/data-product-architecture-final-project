create schema if not exists prediction;

drop table if exists prediction.ejecucion;

create table prediction.ejecucion (
  "pred_date" TEXT,
  "prediction" TEXT,
  "borough" TEXT,
  "model_name" TEXT,
  "model_type" TEXT,
  "schema" TEXT,
  "action" TEXT,
  "creator" TEXT,
  "machine" TEXT,
  "ip" TEXT,
  "date" TEXT,
  "location" TEXT,
  "status" TEXT);

comment on table prediction.ejecucion is 'Tabla para guardar predicciones';
