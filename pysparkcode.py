import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame, DynamicFrameCollection

# -------------------------------
# Transform 1: Clean Visibility
# -------------------------------
def VisibilityMyTransform(glueContext, dfc) -> DynamicFrameCollection:
    from pyspark.sql.functions import col, when, avg, round, lit

    dyf = list(dfc.values())[0]
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

    return DynamicFrameCollection(
        {"output": DynamicFrame.fromDF(df, glueContext, "result")},
        glueContext
    )

# -------------------------------
# Transform 2: Clean Fat Content
# -------------------------------
def FatCleanMyTransform(glueContext, dfc) -> DynamicFrameCollection:
    from pyspark.sql.functions import col, when

    dyf = list(dfc.values())[0]
    df = dyf.toDF()

    df = df.withColumn(
        "fat_content",
        when((col("fat_content") == "LF") | (col("fat_content") == "low fat"), "Low Fat")
        .when(col("fat_content") == "reg", "Regular")
        .otherwise(col("fat_content"))
    )

    return DynamicFrameCollection(
        {"output": DynamicFrame.fromDF(df, glueContext, "result")},
        glueContext
    )

# -------------------------------
# Transform 3: Drop Duplicates
# -------------------------------
def DropDuplicatesMyTransform(glueContext, dfc) -> DynamicFrameCollection:
    dyf = list(dfc.values())[0]
    df = dyf.toDF().dropDuplicates()

    return DynamicFrameCollection(
        {"output": DynamicFrame.fromDF(df, glueContext, "result")},
        glueContext
    )

# -------------------------------
# Transform 4: Change Schema
# -------------------------------
def ChangeSchemaMyTransform(glueContext, dfc) -> DynamicFrameCollection:
    dyf = list(dfc.values())[0]

    mapped_dyf = ApplyMapping.apply(
        frame=dyf,
        mappings=[
            ("Item_Identifier", "string", "item_id", "string"),
            ("Item_Weight", "string", "item_weight", "double"),
            ("Item_Fat_Content", "string", "fat_content", "string"),
            ("Item_Visibility", "string", "visibility", "double"),
            ("Item_Type", "string", "item_type", "string"),
            ("Item_MRP", "string", "mrp", "double"),
            ("Outlet_Identifier", "string", "outlet_id", "string"),
            ("Outlet_Establishment_Year", "string", "est_year", "int"),
            ("Outlet_Size", "string", "outlet_size", "string"),
            ("Outlet_Location_Type", "string", "location_type", "string"),
            ("Outlet_Type", "string", "outlet_type", "string"),
            ("Item_Outlet_Sales", "string", "sales", "double")
        ]
    )

    return DynamicFrameCollection({"output": mapped_dyf}, glueContext)

# -------------------------------
# Transform 5: Handle Nulls
# -------------------------------
def NullMyTransform(glueContext, dfc) -> DynamicFrameCollection:
    from pyspark.sql.functions import col, when, avg, round, lit, count
    from pyspark.sql.window import Window
    from pyspark.sql.functions import row_number

    dyf = list(dfc.values())[0]
    df = dyf.toDF()

    # Fill item_weight
    avg_weight = df.select(avg("item_weight")).collect()[0][0]

    df = df.withColumn(
        "item_weight",
        when(col("item_weight").isNull(), round(lit(avg_weight), 4))
        .otherwise(round(col("item_weight"), 4))
    )

    # Mode calculation for outlet_size
    window_spec = (
        Window.partitionBy("location_type", "outlet_type")
        .orderBy(col("count").desc())
    )

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

    return DynamicFrameCollection(
        {"output": DynamicFrame.fromDF(df, glueContext, "result")},
        glueContext
    )

# -------------------------------
# Main Job
# -------------------------------
args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Read Data
source = glueContext.create_dynamic_frame.from_options(
    format="csv",
    connection_type="s3",
    format_options={"withHeader": True, "separator": ","},
    connection_options={"paths": ["s3://big-market-data/Input_Data/"], "recurse": True}
)

# Pipeline Execution
drop_dup = DropDuplicatesMyTransform(glueContext, DynamicFrameCollection({"df": source}, glueContext))
schema = ChangeSchemaMyTransform(glueContext, drop_dup)
fat_clean = FatCleanMyTransform(glueContext, schema)
visibility = VisibilityMyTransform(glueContext, fat_clean)
final_df = NullMyTransform(glueContext, visibility)

job.commit()
