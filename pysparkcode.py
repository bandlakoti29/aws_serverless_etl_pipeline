import sys
from awsglue.transforms import *
from awsgluedq.transforms import EvaluateDataQuality
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame, DynamicFrameCollection

def SourceNode(glueContext):
    return glueContext.create_dynamic_frame.from_catalog(
        database="big_market_db",
        table_name="input_data",
        transformation_ctx="source"
    )


def DropDuplicatesNode(glueContext, dyf):
    df = dyf.toDF().dropDuplicates()
    return DynamicFrame.fromDF(df, glueContext, "dropdup")


def SchemaNode(glueContext, dyf):
    from pyspark.sql.functions import col

    df = dyf.toDF().select(
        col("Item_Identifier").alias("item_id"),
        col("Item_Weight").cast("double").alias("item_weight"),
        col("Item_Fat_Content").alias("fat_content"),
        col("Item_Visibility").cast("double").alias("visibility"),
        col("Item_Type").alias("item_type"),
        col("Item_MRP").cast("double").alias("mrp"),
        col("Outlet_Identifier").alias("outlet_id"),
        col("Outlet_Establishment_Year").cast("int").alias("est_year"),
        col("Outlet_Size").alias("outlet_size"),
        col("Outlet_Location_Type").alias("location_type"),
        col("Outlet_Type").alias("outlet_type"),
        col("Item_Outlet_Sales").cast("double").alias("sales")
    )

    return DynamicFrame.fromDF(df, glueContext, "schema")


def FatCleanNode(glueContext, dyf):
    from pyspark.sql.functions import col, when

    df = dyf.toDF()

    df = df.withColumn(
        "fat_content",
        when((col("fat_content") == "LF") | (col("fat_content") == "low fat"), "Low Fat")
        .when(col("fat_content") == "reg", "Regular")
        .otherwise(col("fat_content"))
    )

    return DynamicFrame.fromDF(df, glueContext, "fatclean")


def VisibilityNode(glueContext, dyf):
    from pyspark.sql.functions import col, when, avg, round, lit

    df = dyf.toDF()

    df = df.withColumn("visibility", col("visibility") * 100)

    avg_visibility = df.filter(col("visibility") != 0) \
                       .select(avg("visibility")) \
                       .collect()[0][0]

    df = df.withColumn(
        "visibility",
        when(col("visibility") == 0, round(lit(avg_visibility), 4))
        .otherwise(round(col("visibility"), 4))
    )

    return DynamicFrame.fromDF(df, glueContext, "visibility")


def NullNode(glueContext, dyf):
    from pyspark.sql.functions import col, when, avg, round, lit, count
    from pyspark.sql.window import Window
    from pyspark.sql.functions import row_number

    df = dyf.toDF()

    # item_weight
    avg_weight = df.select(avg("item_weight")).collect()[0][0]

    df = df.withColumn(
        "item_weight",
        when(col("item_weight").isNull(), round(lit(avg_weight), 4))
        .otherwise(round(col("item_weight"), 4))
    )

    # outlet_size mode
    window_spec = Window.partitionBy("location_type", "outlet_type") \
                        .orderBy(col("count").desc())

    mode_df = (
        df.filter(col("outlet_size").isNotNull())
        .groupBy("location_type", "outlet_type", "outlet_size")
        .agg(count("*").alias("count"))
        .withColumn("rn", row_number().over(window_spec))
        .filter(col("rn") == 1)
        .drop("count", "rn")
        .withColumnRenamed("outlet_size", "filled_size")
    )

    df = df.join(mode_df, ["location_type", "outlet_type"], "left")

    df = df.withColumn(
        "outlet_size",
        when(col("outlet_size").isNull(), col("filled_size"))
        .otherwise(col("outlet_size"))
    ).drop("filled_size")

    return DynamicFrame.fromDF(df, glueContext, "nullhandled")


def DataQualityNode(glueContext, dyf):
    Rules = """
    Rules = [
        ColumnCount > 10,
        Completeness "item_weight" > 0.9,
        IsUnique "item_id"
    ]
    """

    dq = EvaluateDataQuality().process_rows(
        frame=dyf,
        ruleset=Rules,
        publishing_options={"dataQualityEvaluationContext": "dq_check"},
        additional_options={"performanceTuning.caching": "CACHE_NOTHING"}
    )

    passed = dq.select("rowLevelOutcomes")
    failed = dq.select("ruleOutcomes")

    return passed, failed


def S3TargetNode(glueContext, dyf):
    glueContext.write_dynamic_frame.from_options(
        frame=dyf,
        connection_type="s3",
        format="parquet",
        connection_options={
            "path": "s3://big-market-data/clean-output/",
            "partitionKeys": ["outlet_type", "location_type"]
        },
        transformation_ctx="s3_target"
    )


def CatalogTargetNode(glueContext, dyf):
    glueContext.write_dynamic_frame.from_catalog(
        frame=dyf,
        database="big_market_db",
        table_name="processed_output",
        additional_options={
            "enableUpdateCatalog": True,
            "partitionKeys": ["outlet_type", "location_type"]
        },
        transformation_ctx="catalog_target"
    )


args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
job = Job(glueContext)
job.init(args["JOB_NAME"], args)


source = SourceNode(glueContext)
dropdup = DropDuplicatesNode(glueContext, source)
schema = SchemaNode(glueContext, dropdup)
fat = FatCleanNode(glueContext, schema)
visibility = VisibilityNode(glueContext, fat)
cleaned = NullNode(glueContext, visibility)

# Branch: Data Quality
passed, failed = DataQualityNode(glueContext, cleaned)

# Targets
S3TargetNode(glueContext, passed)
CatalogTargetNode(glueContext, passed)

job.commit()
