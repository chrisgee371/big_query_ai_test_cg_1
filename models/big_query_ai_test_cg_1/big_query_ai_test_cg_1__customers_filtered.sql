{{
  config({
    "materialized": "ephemeral"
  })
}}

WITH customers_source AS (
  SELECT *
  FROM {{ source('prophecy_field_chris_demos', 'customers') }}
),

customers_filtered AS (
  SELECT *
  FROM customers_source
  WHERE status = 'active' OR {{ var('include_inactive_customers') }} = True
),

customers_selected AS (
  SELECT
    customer_id,
    customer_name,
    email,
    region_id
  FROM customers_filtered
)

SELECT * FROM customers_selected
