from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *

# Pipeline configuration parameters (optional - for Analysis UI)
params = Parameters(
    min_order_amount=100,
    include_inactive_customers=False,
    report_date=DateParam("'2024-01-01'")
)

args = PipelineArgs(
    label="big_query_ai_test_cg_1",
    version=1,
    params=params
)

with Pipeline(args) as pipeline:

    # =========================================================================
    # SOURCE GEMS (3 different sources)
    # =========================================================================

    # Source 1: Seed data (CSV file in /seeds/)
    regions_seed = Process(
        name="regions_seed",
        properties=Dataset(table=Dataset.DBTSource(name="regions", sourceType="Seed")),
        input_ports=None
    )

    # Source 2: Warehouse table
    customers_source = Process(
        name="customers_source",
        properties=Dataset(table=Dataset.DBTSource(name="customers", sourceType="Source")),
        input_ports=None
    )

    # Source 3: Another warehouse table
    orders_source = Process(
        name="orders_source",
        properties=Dataset(table=Dataset.DBTSource(name="orders", sourceType="Source")),
        input_ports=None
    )

    # =========================================================================
    # TRANSFORM GEMS (various operations)
    # =========================================================================

    # Reformat: Filter and select columns from customers
    sales_analysis__customers_filtered = Process(
        name="big_query_ai_test_cg_1__customers_filtered",
        properties=ModelTransform(modelName="big_query_ai_test_cg_1__customers_filtered"),
        input_ports=["in_0"],
        output_ports=["out_0"]
    )

    # Join: Customers + Orders (2 inputs)
    sales_analysis__customer_orders = Process(
        name="big_query_ai_test_cg_1__customer_orders",
        properties=ModelTransform(modelName="big_query_ai_test_cg_1__customer_orders"),
        input_ports=["in_0", "in_1"],
        output_ports=["out_0"]
    )

    # Join: Add region data (2 inputs)
    sales_analysis__orders_with_region = Process(
        name="big_query_ai_test_cg_1__orders_with_region",
        properties=ModelTransform(modelName="big_query_ai_test_cg_1__orders_with_region"),
        input_ports=["in_0", "in_1"],
        output_ports=["out_0"]
    )

    # Aggregate: Summarize by region
    sales_analysis__sales_by_region = Process(
        name="big_query_ai_test_cg_1__sales_by_region",
        properties=ModelTransform(modelName="big_query_ai_test_cg_1__sales_by_region"),
        input_ports=["in_0"],
        output_ports=["out_0"]
    )

    # OrderBy + Limit: Top regions
    sales_analysis__top_regions = Process(
        name="big_query_ai_test_cg_1__top_regions",
        properties=ModelTransform(modelName="big_query_ai_test_cg_1__top_regions"),
        input_ports=["in_0"],
        output_ports=["out_0"]
    )

    # =========================================================================
    # TARGET GEM (writes to warehouse table)
    # =========================================================================

    # Final output - materialized as table
    sales_analysis__final_report = Process(
        name="big_query_ai_test_cg_1__final_report",
        properties=ModelTransform(modelName="big_query_ai_test_cg_1__final_report"),
        input_ports=["in_0"],
        output_ports=["out_0"]
    )

    # =========================================================================
    # VISUALIZE GEM (for Analysis dashboard)
    # =========================================================================

    push_to_analysis = Process(
        name="regional_sales_summary",
        properties=Visualize(),
        input_ports=["in_0"],
        output_ports=[]
    )

    # =========================================================================
    # CONNECTIONS (data flow)
    # =========================================================================

    # Filter customers first
    customers_source >> sales_analysis__customers_filtered

    # Join filtered customers with orders
    sales_analysis__customers_filtered >> sales_analysis__customer_orders._in(0)
    orders_source >> sales_analysis__customer_orders._in(1)

    # Join with regions
    sales_analysis__customer_orders >> sales_analysis__orders_with_region._in(0)
    regions_seed >> sales_analysis__orders_with_region._in(1)

    # Aggregate
    sales_analysis__orders_with_region >> sales_analysis__sales_by_region

    # Branch: One path to top regions, one to visualize
    (
        sales_analysis__sales_by_region._out(0)
        >> [sales_analysis__top_regions._in(0), push_to_analysis._in(0)]
    )

    # Final output
    sales_analysis__top_regions >> sales_analysis__final_report
