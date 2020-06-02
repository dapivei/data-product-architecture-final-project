create schema if not exists biasfairness;

drop table if exists biasfairness.aequitas_ejecucion;

create table biasfairness.aequitas_ejecucion (
  "model_name" TEXT,
  "model_id" TEXT,
  "score_threshold" TEXT,
  "k" TEXT,
  "attribute_name" TEXT,
  "attribute_value" TEXT,
  "tpr" TEXT,
  "tnr" TEXT,
  "fxr" TEXT,
  "fdr" TEXT,
  "fpr" TEXT,
  "fnr" TEXT,
  "npv" TEXT,
  "precision" TEXT,
  "pp" TEXT,
  "pn" TEXT,
  "ppr" TEXT,
  "pprev" TEXT,
  "fp" TEXT,
  "fn" TEXT,
  "tn" TEXT,
  "tp" TEXT,
  "group_label_pos" TEXT,
  "group_label_neg" TEXT,
  "group_size" TEXT,
  "total_entities" TEXT,
  "prev" TEXT
);

comment on table biasfairness.aequitas_ejecucion is 'Metadata from Bias and Fairness using Aequitas';
