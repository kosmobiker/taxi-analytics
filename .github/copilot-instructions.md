# GitHub Copilot Instructions for Taxi Analytics Project

This document provides guidance for GitHub Copilot to assist in developing the Taxi Analytics project, which compares Python and Rust implementations for processing NYC Taxi data.

## Project Overview

This project aims to compare Python and Rust implementations for processing large-scale NYC Taxi & Limousine Commission data, focusing on performance metrics and data engineering capabilities.

## Data Source Understanding

When working with NYC TLC data:
- Format: Parquet/CSV files containing taxi trip records
- Size: 2-4GB per month, targeting 20-30GB total
- Source: NYC Taxi & Limousine Commission (TLC) Trip Record Data
- Schema: Focus on key fields for trips, fares, locations, and timestamps

## Code Organization Principles

When suggesting code:
1. Maintain parallel implementations in both languages
2. Keep directory structure:
   ```
   taxi-analytics/
   ├── data/                 # Raw datasets
   ├── python-etl/          # Python implementation
   ├── rust-etl/            # Rust implementation  
   ├── sql/                 # ClickHouse schemas
   ├── notebooks/           # Analysis & benchmarks
   ├── docker-compose.yml   # Local Metabase setup
   └── README.md           # Performance results
   ```

## Implementation Guidelines

### Python ETL Implementation
- Suggest modern Python practices (3.9+)
- Utilize pandas, polars, or similar performant libraries
- Implement proper error handling and logging
- Focus on memory-efficient processing
- Include type hints and documentation

### Rust ETL Implementation
- Suggest idiomatic Rust patterns
- Utilize appropriate crates for CSV/Parquet processing
- Implement proper error handling with Result types
- Focus on safe concurrent processing
- Include comprehensive documentation

## Performance Testing Areas

When suggesting test implementations, focus on:

1. Data Ingestion:
   - CSV to Parquet conversion
   - Parallel processing capabilities
   - Batch processing optimization

2. Aggregations:
   - Trip counts by time periods
   - Fare calculations by zones
   - Memory-efficient grouping operations

3. Time Series Processing:
   - Rolling averages calculation
   - Seasonal pattern analysis
   - Efficient timestamp handling

4. Geospatial Operations:
   - Distance calculations
   - Zone mapping and validation
   - Coordinate transformations

5. Complex Joins:
   - Trip data with zone lookups
   - Weather data integration
   - Optimization for large datasets

## Metrics Collection

When implementing performance measurements:

1. Processing Time:
   - Total pipeline execution time
   - Individual operation timing
   - Startup/initialization overhead

2. Memory Usage:
   - Peak memory consumption
   - Memory usage patterns
   - Garbage collection impact (Python)

3. CPU Utilization:
   - Multi-core efficiency
   - Thread usage patterns
   - CPU load distribution

4. I/O Performance:
   - Read/write throughput
   - Disk operation patterns
   - Buffer usage efficiency

## Database Integration

When working with ClickHouse:
- Suggest optimal table schemas
- Include proper indexing strategies
- Implement efficient data loading patterns
- Focus on query optimization

## Visualization Integration

When suggesting visualization code, focus on implementing and comparing multiple BI tools:

### Metabase Setup
- Implement using Docker container setup
- Configure ClickHouse connection
- Create performance comparison dashboards
- Design trip analysis visualizations
- Document custom SQL queries

### Apache Superset Setup
- Deploy using Docker Compose
- Configure ClickHouse connection settings
- Create interactive dashboards
- Implement geospatial visualizations
- Document chart configurations

### Additional BI Tools
Consider integration with:
- Grafana (Docker setup)
- Redash (Docker setup)
- PowerBI (Desktop version)

### Dashboard Requirements
For each BI tool implementation:
- Performance metrics visualization
- Time series analysis dashboards
- Geospatial heat maps
- A/B test comparisons
- Cross-tool performance analysis

### Docker Compose Configuration
Maintain configurations for:
- Multi-container BI tool setups
- Network configuration
- Volume persistence
- Environment variables
- Health checks

## Testing Guidelines

Suggest tests that cover:
- Data validation
- Performance benchmarks
- Error handling
- Edge cases
- Memory leak detection

## Documentation Requirements

When suggesting documentation:
- Include clear function/method descriptions
- Provide usage examples
- Document performance characteristics
- Include setup instructions
- Detail test procedures

## Error Handling

Suggest error handling that:
- Provides clear error messages
- Implements proper recovery strategies
- Logs relevant debug information
- Maintains data consistency

Remember to maintain parallel functionality between Python and Rust implementations while leveraging each language's strengths for optimal performance.
