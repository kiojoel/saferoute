-- Enable PostGIS extension
CREATE EXTENSION IF NOT EXISTS postgis;

CREATE TABLE countries (
    id      SERIAL PRIMARY KEY,
    name    TEXT      NOT NULL,
    iso_a2  CHAR(2)   NOT NULL UNIQUE,
    iso_a3  CHAR(3)   NOT NULL UNIQUE
);

INSERT INTO countries (name, iso_a2, iso_a3)
VALUES ('Nigeria', 'NG', 'NGA');

CREATE TABLE admin_units (
    id          SERIAL PRIMARY KEY,
    country_id  INTEGER NOT NULL REFERENCES countries(id),
    code        TEXT    NOT NULL UNIQUE,
    name        TEXT    NOT NULL,
    level       INTEGER NOT NULL,
    parent_code TEXT,
    geometry    GEOMETRY(MULTIPOLYGON, 4326) NOT NULL
);

CREATE INDEX admin_units_geometry_idx  ON admin_units USING GIST(geometry);
CREATE INDEX admin_units_country_idx   ON admin_units(country_id);
CREATE INDEX admin_units_level_idx     ON admin_units(level);


CREATE TABLE conflict_events (
    id            SERIAL PRIMARY KEY,
    source        TEXT    NOT NULL,
    source_id     TEXT,
    country_id    INTEGER REFERENCES countries(id),
    admin_unit_id INTEGER REFERENCES admin_units(id),
    event_date    DATE    NOT NULL,
    event_type    TEXT    NOT NULL,
    sub_event_type TEXT,
    location_name TEXT,
    latitude      DOUBLE PRECISION NOT NULL,
    longitude     DOUBLE PRECISION NOT NULL,
    geometry      GEOMETRY(POINT, 4326),
    fatalities    INTEGER NOT NULL DEFAULT 0,
    description   TEXT,
    source_url    TEXT,
    ingested_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(source, source_id)
);

CREATE INDEX conflict_events_geometry_idx    ON conflict_events USING GIST(geometry);
CREATE INDEX conflict_events_date_idx        ON conflict_events(event_date);
CREATE INDEX conflict_events_admin_unit_idx  ON conflict_events(admin_unit_id);
CREATE INDEX conflict_events_country_idx     ON conflict_events(country_id);


CREATE TABLE risk_scores (
    id                  SERIAL PRIMARY KEY,
    admin_unit_id       INTEGER NOT NULL REFERENCES admin_units(id),
    score               NUMERIC(5,2) NOT NULL CHECK (score >= 0 AND score <= 100),
    tier                TEXT NOT NULL CHECK (
                            tier IN ('safe','caution','high_risk','do_not_travel')
                        ),
    incident_count_30d  INTEGER NOT NULL DEFAULT 0,
    incident_count_90d  INTEGER NOT NULL DEFAULT 0,
    fatality_count_30d  INTEGER NOT NULL DEFAULT 0,
    top_factors         JSONB,
    model_version       TEXT NOT NULL,
    data_through        DATE NOT NULL,
    computed_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX risk_scores_admin_unit_idx  ON risk_scores(admin_unit_id);
CREATE INDEX risk_scores_computed_at_idx ON risk_scores(computed_at DESC);

-
CREATE TABLE data_sources (
    id              SERIAL PRIMARY KEY,
    name            TEXT NOT NULL UNIQUE,
    source_type     TEXT NOT NULL,
    last_ingested_at TIMESTAMPTZ,
    record_count    INTEGER,
    notes           TEXT
);