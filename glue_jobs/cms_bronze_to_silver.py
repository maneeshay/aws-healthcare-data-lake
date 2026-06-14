"""
Bronze to Silver Transformation Script
CMS Medicare Provider Data Pipeline

Reads raw CSV from S3 Bronze, transforms it locally using PySpark,
and writes Parquet files back to S3 Silver partitioned by state.
"""

import boto3
import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import FloatType

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration
BUCKET          = "aws-healthcare-data-lake"
BRONZE_KEY      = "bronze/cms_providers/year=2024/Medicare_Physician_Other_Practitioners_by_Provider_2024.csv"
SILVER_PREFIX   = "silver/cms_providers"
LOCAL_INPUT     = "/tmp/cms_bronze.csv"
LOCAL_OUTPUT    = "/tmp/cms_silver"
AWS_REGION      = "us-east-2"


def download_from_s3():
    """Download Bronze CSV from S3 to local /tmp directory."""
    logger.info("Downloading Bronze CSV from S3...")
    s3 = boto3.client("s3", region_name=AWS_REGION)
    s3.download_file(BUCKET, BRONZE_KEY, LOCAL_INPUT)
    logger.info(f"Downloaded to {LOCAL_INPUT}")


def create_spark_session():
    """Create a local Spark session — no S3 connectivity needed."""
    spark = (
        SparkSession.builder
        .appName("CMS Bronze to Silver")
        .master("local[*]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_bronze(spark):
    """Read the raw CSV from local disk."""
    logger.info("Reading Bronze CSV...")
    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .option("quote", '"')
        .option("escape", '"')
        .csv(LOCAL_INPUT)
    )
    logger.info(f"Bronze row count: {df.count()}")
    return df


def rename_columns(df):
    """Rename CMS abbreviation columns to readable snake_case."""
    column_mapping = {
        "Rndrng_NPI":                     "provider_npi",
        "Rndrng_Prvdr_Last_Org_Name":     "provider_last_org_name",
        "Rndrng_Prvdr_First_Name":        "provider_first_name",
        "Rndrng_Prvdr_MI":                "provider_middle_initial",
        "Rndrng_Prvdr_Crdntls":           "provider_credentials",
        "Rndrng_Prvdr_Gndr":              "provider_gender",
        "Rndrng_Prvdr_Ent_Cd":            "provider_entity_code",
        "Rndrng_Prvdr_St1":               "provider_street1",
        "Rndrng_Prvdr_St2":               "provider_street2",
        "Rndrng_Prvdr_City":              "provider_city",
        "Rndrng_Prvdr_State_Abrvtn":      "provider_state",
        "Rndrng_Prvdr_State_FIPS":        "provider_state_fips",
        "Rndrng_Prvdr_Zip5":              "provider_zip",
        "Rndrng_Prvdr_RUCA":              "provider_ruca_code",
        "Rndrng_Prvdr_RUCA_Desc":         "provider_ruca_desc",
        "Rndrng_Prvdr_Cntry":             "provider_country",
        "Rndrng_Prvdr_Type":              "provider_type",
        "Rndrng_Prvdr_Mdcr_Prtcptg_Ind": "provider_medicare_participating",
        "Tot_HCPCS_Cds":                  "total_hcpcs_codes",
        "Tot_Benes":                      "total_beneficiaries",
        "Tot_Srvcs":                      "total_services",
        "Tot_Sbmtd_Chrg":                 "total_submitted_charge",
        "Tot_Mdcr_Alowd_Amt":             "total_medicare_allowed_amt",
        "Tot_Mdcr_Pymt_Amt":              "total_medicare_payment_amt",
        "Tot_Mdcr_Stdzd_Amt":             "total_medicare_standardized_amt"
    }
    for old_name, new_name in column_mapping.items():
        df = df.withColumnRenamed(old_name, new_name)
    logger.info("Columns renamed to snake_case.")
    return df


def filter_invalid_rows(df):
    """Remove rows where provider_npi is null or empty."""
    before = df.count()
    df = df.filter(
        F.col("provider_npi").isNotNull() &
        (F.col("provider_npi") != "")
    )
    after = df.count()
    logger.info(f"Filtered {before - after} rows with null/empty NPI. Remaining: {after}")
    return df


def cast_numeric_columns(df):
    """Cast payment and count columns from string to float."""
    numeric_columns = [
        "total_hcpcs_codes",
        "total_beneficiaries",
        "total_services",
        "total_submitted_charge",
        "total_medicare_allowed_amt",
        "total_medicare_payment_amt",
        "total_medicare_standardized_amt"
    ]
    for col in numeric_columns:
        df = df.withColumn(col, F.col(col).cast(FloatType()))
    logger.info("Numeric columns cast to float.")
    return df


def deduplicate(df):
    """Keep only one row per provider NPI."""
    before = df.count()
    df = df.dropDuplicates(["provider_npi"])
    after = df.count()
    logger.info(f"Removed {before - after} duplicate NPIs. Remaining: {after}")
    return df


def add_derived_columns(df):
    """Add payment_per_service derived column."""
    df = df.withColumn(
        "payment_per_service",
        F.when(
            F.col("total_services") > 0,
            F.col("total_medicare_payment_amt") / F.col("total_services")
        ).otherwise(None)
    )
    logger.info("Derived column payment_per_service added.")
    return df


def write_silver_local(df):
    """Write transformed Parquet files to local /tmp directory."""
    logger.info(f"Writing Silver Parquet to {LOCAL_OUTPUT}...")
    (
        df.write
        .mode("overwrite")
        .partitionBy("provider_state")
        .parquet(LOCAL_OUTPUT)
    )
    logger.info("Silver written locally.")


def upload_to_s3():
    """Upload all local Silver Parquet files to S3."""
    logger.info("Uploading Silver Parquet files to S3...")
    s3 = boto3.client("s3", region_name=AWS_REGION)
    for root, dirs, files in os.walk(LOCAL_OUTPUT):
        for file in files:
            local_path = os.path.join(root, file)
            relative_path = os.path.relpath(local_path, LOCAL_OUTPUT)
            s3_key = f"{SILVER_PREFIX}/{relative_path}"
            s3.upload_file(local_path, BUCKET, s3_key)
            logger.info(f"Uploaded {s3_key}")
    logger.info("All Silver files uploaded to S3.")


def main():
    # Step 1: Download Bronze CSV from S3
    download_from_s3()

    # Step 2: Create local Spark session
    spark = create_spark_session()

    # Step 3: Read raw Bronze data
    df = read_bronze(spark)

    # Step 4: Rename columns to snake_case
    df = rename_columns(df)

    # Step 5: Remove rows with null NPI
    df = filter_invalid_rows(df)

    # Step 6: Cast string columns to numeric types
    df = cast_numeric_columns(df)

    # Step 7: Deduplicate on provider NPI
    df = deduplicate(df)

    # Step 8: Add derived analytical column
    df = add_derived_columns(df)

    # Step 9: Write clean Parquet to local disk
    write_silver_local(df)

    # Step 10: Upload Silver Parquet files to S3
    upload_to_s3()

    logger.info("Bronze to Silver transformation complete.")
    spark.stop()


if __name__ == "__main__":
    main()