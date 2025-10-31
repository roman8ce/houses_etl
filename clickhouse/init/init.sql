CREATE DATABASE IF NOT EXISTS houses;

CREATE TABLE IF NOT EXISTS houses.houses_rf(
    house_id UInt32,
    latitude Nullable(Float64),
    longitude Nullable(Float64),
    maintenance_year Nullable(UInt16),
    square Nullable(Float64),
    population Nullable(UInt16),
    region Nullable(String),
    locality_name Nullable(String),
    address Nullable(String),
    full_address Nullable(String),
    communal_service_id Nullable(Float64),
    description Nullable(String)
)
ENGINE = MergeTree()
ORDER BY house_id;