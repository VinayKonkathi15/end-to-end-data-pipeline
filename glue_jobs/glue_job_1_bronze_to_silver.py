from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, lit, current_timestamp,
    abs as spark_abs
)

spark = SparkSession.builder.getOrCreate()

# =========================
# CONFIG
# =========================
BUCKET = "end-2-end-bucket-vinay"
DATASET = "sales"
INGESTION_DATE = "2025-12-22"
TOLERANCE = 0.01

BRONZE_PATH = f"s3://end-2-end-bucket-vinay/bronze/sales/"
SILVER_PATH = f"s3://end-2-end-bucket-vinay/silver/sales/"
QUARANTINE_PATH = f"s3://end-2-end-bucket-vinay/quarantine/sales/"
AUDIT_PATH = f"s3://end-2-end-bucket-vinay/audit/sales/"

# =========================
# READ BRONZE DATA
# =========================
df = spark.read.option("header", "true").csv(BRONZE_PATH)

total_records = df.count()

# =========================
# CRITICAL RULE 1
# ORDERNUMBER NOT NULL
# =========================
c1_failed_count = df.filter(col("ORDERNUMBER").isNull()).count()

# =========================
# CRITICAL RULE 2
# PK UNIQUENESS
# =========================
dup_df = (
    df.groupBy("ORDERNUMBER", "ORDERLINENUMBER")
      .count()
      .filter(col("count") > 1)
)

c2_failed_count = dup_df.count()

# =========================
# CRITICAL RULE 3
# SALES VALIDATION (CONDITIONAL)
# =========================
revenue_check_df = df.filter(
    col("QUANTITYORDERED").isNotNull() &
    col("PRICEEACH").isNotNull() &
    col("SALES").isNotNull()
)

revenue_mismatch_df = revenue_check_df.filter(
    spark_abs(
        col("SALES").cast("double") -
        (col("QUANTITYORDERED").cast("double") * col("PRICEEACH").cast("double"))
    ) > TOLERANCE
)

c3_failed_count = revenue_mismatch_df.count()

# =========================
# PIPELINE DECISION
# =========================
pipeline_failed = (
    total_records == 0 or
    c1_failed_count > 0 or
    c2_failed_count > 0 or
    c3_failed_count > 0
)

# =========================
# NON-CRITICAL RULES
# QUARANTINE DATA
# =========================
quarantine_df = df.withColumn(
    "dq_failed_rule",
    when(col("ORDERLINENUMBER").isNull(), lit("N1_ORDERLINENUMBER_NULL"))
    .when(col("QUANTITYORDERED").isNull(), lit("N2_QUANTITYORDERED_NULL"))
    .when(col("PRICEEACH").isNull(), lit("N3_PRICEEACH_NULL"))
    .when(col("SALES").isNull(), lit("N4_SALES_NULL"))
    .when(col("MONTH_ID").isNull(), lit("N5_MONTH_ID_NULL"))
    .when(col("QTR_ID").isNull(), lit("N6_QTR_ID_NULL"))
)

quarantine_df = quarantine_df.filter(col("dq_failed_rule").isNotNull()) \
    .withColumn("dq_rule_type", lit("NON_CRITICAL")) \
    .withColumn("dq_run_timestamp", current_timestamp()) \
    .withColumn("ingestion_date", lit(INGESTION_DATE))

# =========================
# WRITE QUARANTINE
# =========================
quarantine_df.write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(QUARANTINE_PATH)


# =========================
# SILVER DATA
# =========================
silver_df = df.filter(col("ORDERNUMBER").isNotNull()) \
    .join(
        quarantine_df.select("ORDERNUMBER", "ORDERLINENUMBER"),
        on=["ORDERNUMBER", "ORDERLINENUMBER"],
        how="left_anti"
    )

silver_df.write.mode("overwrite").parquet(SILVER_PATH)

# =========================
# AUDIT METRICS
# =========================
audit_data = [
    ("C1_ORDERNUMBER_NOT_NULL", "CRITICAL", c1_failed_count),
    ("C2_PK_UNIQUENESS", "CRITICAL", c2_failed_count),
    ("C3_SALES_VALIDATION", "CRITICAL", c3_failed_count),
    ("N1_ORDERLINENUMBER_NULL", "NON_CRITICAL", df.filter(col("ORDERLINENUMBER").isNull()).count()),
    ("N2_QUANTITYORDERED_NULL", "NON_CRITICAL", df.filter(col("QUANTITYORDERED").isNull()).count()),
    ("N3_PRICEEACH_NULL", "NON_CRITICAL", df.filter(col("PRICEEACH").isNull()).count()),
    ("N4_SALES_NULL", "NON_CRITICAL", df.filter(col("SALES").isNull()).count()),
    ("N5_MONTH_ID_NULL", "NON_CRITICAL", df.filter(col("MONTH_ID").isNull()).count()),
    ("N6_QTR_ID_NULL", "NON_CRITICAL", df.filter(col("QTR_ID").isNull()).count())
]

audit_df = spark.createDataFrame(
    audit_data,
    ["rule_id", "rule_type", "failed_records"]
).withColumn("total_records", lit(total_records)) \
 .withColumn("dq_run_timestamp", current_timestamp()) \
 .withColumn("ingestion_date", lit(INGESTION_DATE))

audit_df.write \
    .mode("append") \
    .option("header", "true") \
    .csv(AUDIT_PATH)


import boto3

# =========================
# TRIGGER GLUE JOB 2
# =========================
glue_client = boto3.client("glue")

glue_client.start_job_run(
    JobName="sales_silver_to_gold"
)


# =========================
# FINAL STATUS
# =========================
if pipeline_failed:
    raise Exception("❌ Pipeline failed due to CRITICAL data quality issues")
# CI test comment
#Testing 
print("✅ Glue Job completed successfully")
