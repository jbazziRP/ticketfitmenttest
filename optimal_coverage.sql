{% macro test_coverage_fitment(model, column_name, coverage_threshold, severity) %}

-- Step 1: Count total parts in the catalog
WITH total_parts AS (
    SELECT COUNT(*) AS total_count
    FROM {{ model }}
),

-- Step 2: Count parts that have a valid image URL
parts_with_images AS (
    SELECT COUNT(*) AS image_count
    FROM {{ model }}
    WHERE {{ column_name }} IS NOT NULL 
    AND LENGTH({{ column_name }}) > 0 -- Ensures empty strings are not counted as valid images
),

-- Step 3: Compute coverage percentage and assign a status
coverage_analysis AS (
    SELECT 
        t.total_count,
        p.image_count,
        (p.image_count * 100.0 / NULLIF(t.total_count, 0)) AS coverage_percentage,
        CASE 
            WHEN (p.image_count * 100.0 / NULLIF(t.total_count, 0)) >= {{ coverage_threshold }} THEN 'PASS'
            WHEN (p.image_count * 100.0 / NULLIF(t.total_count, 0)) >= 60 THEN 'WARN'
            ELSE 'FAIL'
        END AS status
    FROM total_parts t
    CROSS JOIN parts_with_images p
)

-- Step 4: Output the results, including a timestamp for tracking
SELECT 
    total_count,
    image_count,
    coverage_percentage,
    status,
    CAST(NOW() AS VARCHAR) AS analysis_timestamp -- Fixes Athena's timestamp issue
FROM coverage_analysis;

{% endmacro %}