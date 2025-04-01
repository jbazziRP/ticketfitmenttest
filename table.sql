{{ config(
    materialized='table',  -- Ensures this is stored as a table, not just a view
    location='s3://your-bucket-name/dc_fitment_coverage_analysis/',
    format='parquet',
    partition_by=['status'],  -- Optimizes for partitioned querying
    table_properties={
        'compression': 'SNAPPY',  -- Speeds up query performance
        'external_table': 'TRUE'
    }
) }}

-- Generate Coverage Fitment Data
WITH coverage_data AS (
    {{ test_coverage_fitment(ref('dc_catalog'), 'image_url', 80, 'fail') }}
),

-- Generate Additional Metrics
detailed_metrics AS (
    SELECT 
        c.*,
        (c.image_count * 1.0 / NULLIF(c.total_count, 1)) AS decimal_coverage, -- Avoids 100% rounding
        CASE 
            WHEN c.coverage_percentage >= 90 THEN 'Excellent Coverage'
            WHEN c.coverage_percentage >= 80 THEN 'Good Coverage'
            WHEN c.coverage_percentage >= 60 THEN 'Moderate Coverage'
            ELSE 'Poor Coverage'
        END AS coverage_description,
        CAST(NOW() AS VARCHAR) AS analysis_timestamp
    FROM coverage_data c
)

-- Final Table Output
SELECT * FROM detailed_metrics;