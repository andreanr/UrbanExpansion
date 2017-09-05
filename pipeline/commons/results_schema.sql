-- model table containing each of the models run.
CREATE TABLE results.models (
  model_id              SERIAL PRIMARY KEY,
  city                  TEXT,
  run_time              TIMESTAMP default current_timestamp,
  model_type            TEXT,
  model_parameters      JSONB,
  features              TEXT[],
  years_train           INT[],
  grid_size             INT,
  built_threshold       NUMERIC,
  population_threshold  NUMERIC,
  cluster_threshold     NUMERIC,
  label_range           INT[],
  model_comment         TEXT
);

-- predictions table
CREATE TABLE results.predictions (
  model_id       INT REFERENCES results.models (model_id),
  city           TEXT,
  year_features  INT,
  cell_id        INT,
  score          REAL,
  label          BOOL
);

-- feature importance table
CREATE TABLE results.feature_importances (
  model_id             INT REFERENCES results.models (model_id),
  city                 TEXT,
  feature              TEXT,
  feature_importance   REAL,
  rank_abs             INT
);

CREATE TABLE results.evaluations (
  model_id            INT REFERENCES results.models (model_id),
  city                TEXT,
  years               INT[],
  type_validation     TEXT,
  metric              TEXT,
  cutoff              TEXT,
  value               REAL
);
