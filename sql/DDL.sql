-- ClickHouse Table Creation Scripts for NYC Taxi Data
-- Based on ClickHouse best practices for schema design

-- ==============================================
-- YELLOW TAXI TABLE (Optimized Schema)
-- ==============================================
DROP TABLE IF EXISTS yellow_taxi_trips;

CREATE TABLE yellow_taxi_trips (
    -- Primary columns (optimized types)
    VendorID UInt8,
    tpep_pickup_datetime DateTime,
    tpep_dropoff_datetime DateTime,
    passenger_count UInt8 DEFAULT 1,
    trip_distance Float32,
    RatecodeID UInt8 DEFAULT 1,
    store_and_fwd_flag Bool DEFAULT false,
    PULocationID UInt16,
    DOLocationID UInt16,
    payment_type UInt8,
    
    -- Financial columns (Float32 for better compression)
    fare_amount Float32,
    extra Float32,
    mta_tax Float32,
    tip_amount Float32,
    tolls_amount Float32,
    improvement_surcharge Float32,
    total_amount Float32,
    congestion_surcharge Float32 DEFAULT 0,
    Airport_fee Float32 DEFAULT 0,
    cbd_congestion_fee Float32,
    
    -- Derived columns for analytics
    trip_duration_minutes Float32,
    pickup_hour UInt8,
    pickup_day_of_week UInt8,
    pickup_date Date,
    tip_percentage Float32,
    avg_speed_mph Float32
)
ENGINE = MergeTree()
-- Ordering key optimized for common queries: filter by date/hour, then location
ORDER BY (pickup_date, pickup_hour, PULocationID, DOLocationID)
-- Partition by month for better query performance and data management  
PARTITION BY toYYYYMM(tpep_pickup_datetime)
SETTINGS index_granularity = 8192;

-- ==============================================
-- GREEN TAXI TABLE (Optimized Schema)
-- ==============================================
DROP TABLE IF EXISTS green_taxi_trips;

CREATE TABLE green_taxi_trips (
    -- Primary columns (optimized types)
    VendorID UInt8,
    lpep_pickup_datetime DateTime,
    lpep_dropoff_datetime DateTime,
    passenger_count UInt8 DEFAULT 1,
    trip_distance Float32,
    RatecodeID UInt8 DEFAULT 1,
    store_and_fwd_flag Bool DEFAULT false,
    PULocationID UInt16,
    DOLocationID UInt16,
    payment_type UInt8,
    trip_type UInt8,

    -- Financial columns (Float32 for better compression)
    fare_amount Float32,
    extra Float32,
    mta_tax Float32,
    tip_amount Float32,
    tolls_amount Float32,
    improvement_surcharge Float32,
    total_amount Float32,
    congestion_surcharge Float32,
    cbd_congestion_fee Float32,
    
    -- Derived columns for analytics
    trip_duration_minutes Float32,
    pickup_hour UInt8,
    pickup_day_of_week UInt8,
    pickup_date Date,
    tip_percentage Float32,
    avg_speed_mph Float32
)
ENGINE = MergeTree()
-- Ordering key optimized for common queries: filter by date/hour, then location
ORDER BY (pickup_date, pickup_hour, PULocationID, DOLocationID)
-- Partition by month for better query performance and data management
PARTITION BY toYYYYMM(lpep_pickup_datetime)
SETTINGS index_granularity = 8192;

-- ==============================================
-- UNIFIED VIEW FOR CROSS-TAXI ANALYSIS
-- ==============================================
CREATE VIEW IF NOT EXISTS unified_taxi_trips AS
SELECT
    -- Common Columns
    VendorID,
    tpep_pickup_datetime AS pickup_datetime,
    tpep_dropoff_datetime AS dropoff_datetime,
    passenger_count,
    trip_distance,
    RatecodeID,
    store_and_fwd_flag,
    PULocationID,
    DOLocationID,
    payment_type,
    
    -- Financial Columns
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount,
    congestion_surcharge,
    Airport_fee,
    cbd_congestion_fee,

    -- Derived Columns
    trip_duration_minutes,
    pickup_hour,
    pickup_day_of_week,
    pickup_date,
    tip_percentage,
    avg_speed_mph,
    
    -- Identifier
    'yellow' AS taxi_type
FROM yellow_taxi_trips

UNION ALL

SELECT
    -- Common Columns (aliased to match yellow schema)
    VendorID,
    lpep_pickup_datetime AS pickup_datetime,
    lpep_dropoff_datetime AS dropoff_datetime,
    passenger_count,
    trip_distance,
    RatecodeID,
    store_and_fwd_flag,
    PULocationID,
    DOLocationID,
    payment_type,
    
    -- Financial Columns
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount,
    congestion_surcharge,
    
    -- Placeholder for missing columns in green data
    0 AS Airport_fee,
    cbd_congestion_fee,

    -- Derived Columns
    trip_duration_minutes,
    pickup_hour,
    pickup_day_of_week,
    pickup_date,
    tip_percentage,
    avg_speed_mph,
    
    -- Identifier
    'green' AS taxi_type
FROM green_taxi_trips;

-- ==============================================
-- PERFORMANCE INDEXES (Optional but recommended)
-- ==============================================

-- Skip indexes for better filtering performance
ALTER TABLE yellow_taxi_trips ADD INDEX idx_pickup_hour pickup_hour TYPE minmax GRANULARITY 1;
ALTER TABLE yellow_taxi_trips ADD INDEX idx_payment_type payment_type TYPE set(10) GRANULARITY 1;
ALTER TABLE yellow_taxi_trips ADD INDEX idx_fare_amount fare_amount TYPE minmax GRANULARITY 1;
ALTER TABLE yellow_taxi_trips ADD INDEX idx_trip_distance trip_distance TYPE minmax GRANULARITY 1;

ALTER TABLE green_taxi_trips ADD INDEX idx_pickup_hour pickup_hour TYPE minmax GRANULARITY 1;
ALTER TABLE green_taxi_trips ADD INDEX idx_payment_type payment_type TYPE set(10) GRANULARITY 1;
ALTER TABLE green_taxi_trips ADD INDEX idx_fare_amount fare_amount TYPE minmax GRANULARITY 1;
ALTER TABLE green_taxi_trips ADD INDEX idx_trip_distance trip_distance TYPE minmax GRANULARITY 1;

-- ==============================================
-- VERIFICATION QUERIES
-- ==============================================

-- Check table structures
DESCRIBE TABLE yellow_taxi_trips;
DESCRIBE TABLE green_taxi_trips;

-- Check if tables are created
SHOW TABLES LIKE '%taxi%';

-- Sample queries to test after data load
-- SELECT COUNT(*) FROM yellow_taxi_trips;
-- SELECT COUNT(*) FROM green_taxi_trips;
-- SELECT COUNT(*) FROM unified_taxi_trips;

-- Performance test query examples:
-- SELECT pickup_hour, COUNT(*) FROM yellow_taxi_trips GROUP BY pickup_hour ORDER BY pickup_hour;
-- SELECT taxi_type, AVG(trip_distance) FROM unified_taxi_trips GROUP BY taxi_type;