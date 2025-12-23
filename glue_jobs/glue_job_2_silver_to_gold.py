from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum

spark = SparkSession.builder.getOrCreate()

# =========================
# CONFIG
# =========================
BUCKET = "end-2-end-bucket-vinay"
DATASET = "sales"

SILVER_PATH = f"s3://end-2-end-bucket-vinay/silver/sales/"
GOLD_BASE_PATH = f"s3://end-2-end-bucket-vinay/gold/sales/"

# =========================
# READ SILVER DATA
# =========================
df = spark.read.parquet(SILVER_PATH)

# Ensure numeric types
df = df.withColumn("SALES", col("SALES").cast("double"))
df = df.withColumn("YEAR_ID", col("YEAR_ID").cast("int"))

# =========================
# GOLD 1: REVENUE BY YEAR
# =========================
revenue_by_year = (
    df.groupBy("YEAR_ID")
      .agg(spark_sum("SALES").alias("total_revenue"))
)

revenue_by_year.write.mode("overwrite").parquet(
    GOLD_BASE_PATH + "revenue_by_year/"
)

# =========================
# GOLD 2: REVENUE BY PRODUCT LINE
# =========================
revenue_by_productline = (
    df.groupBy("PRODUCTLINE")
      .agg(spark_sum("SALES").alias("total_revenue"))
)

revenue_by_productline.write.mode("overwrite").parquet(
    GOLD_BASE_PATH + "revenue_by_productline/"
)

# =========================
# GOLD 3: REVENUE BY COUNTRY
# =========================
revenue_by_country = (
    df.groupBy("COUNTRY")
      .agg(spark_sum("SALES").alias("total_revenue"))
)

revenue_by_country.write.mode("overwrite").parquet(
    GOLD_BASE_PATH + "revenue_by_country/"
)

import boto3

# =========================
# TRIGGER GOLD CRAWLERS
# =========================
glue_client = boto3.client("glue")

crawler_names = [
    "gold_rev_year_crawler",
    "gold_rev_product_crawler",
    "gold_rev_country_crawler"
]

for crawler in crawler_names:
    glue_client.start_crawler(Name=crawler)

print("✅ Gold crawlers triggered successfully")

