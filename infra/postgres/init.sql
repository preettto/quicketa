-- PostgreSQL "partner service" database: an operational source system for the P6 pipeline.
-- Seeded from the raw dataset in Phase 4 (simulates a separate team's database).

CREATE TABLE IF NOT EXISTS delivery_partners (
    partner_id    VARCHAR(32) PRIMARY KEY,
    city_code     VARCHAR(16) NOT NULL,
    age           SMALLINT,
    rating        NUMERIC(2,1),
    vehicle_type  VARCHAR(32),
    updated_at    TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS restaurants (
    restaurant_id VARCHAR(32) PRIMARY KEY,
    city_code     VARCHAR(16) NOT NULL,
    lat           DOUBLE PRECISION NOT NULL,
    lon           DOUBLE PRECISION NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_partners_city ON delivery_partners(city_code);
CREATE INDEX IF NOT EXISTS idx_restaurants_city ON restaurants(city_code);
