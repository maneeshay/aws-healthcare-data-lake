"""
Bronze to silver Transformation Script
CMS medicare provider data pipeline

This script reads raw cms medicare Provider data from the s3 bronze layer,
applies strandardization and qualty transformation, and write clean parquet files to the s3 silver layer 
partition by state.

Written in Glue-compatible Pyspark style, executed locally.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import FloatType
import logging
import boto3

#Configure logging so we can see what the script is doing as it runs
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

#--------------------------------------------------------------
# CONFIGURATION
# All paths and settings in one place so they are easy to change
# without touching the logic of the code
#--------------------------------------------------------------

BRONZE_PATH = "s3a://aws-healthcare-data-lake/bronze/cms_providers/year=2024/Medicare_Physician_Other_Practitioners_by_Provider_2024.csv"
SILVER_PATH = "S3A://AWS-healthcare-data-lake/silver/cms_providers/"
AWS_REGION = "us-east-2"

def create_spark_session():
    """
    Create a spark session configured to read and write to s3.
    We use the hadoop - aws package which gives spark the ability
    to talk to s3 using the s3a:// protocol.
    """
    import boto3
    import os
    session = boto3.Session()
    credentials = session.get_credentials().get_frozen_credentials()
    home = os.path.expanduser("~")
    jars = f"{home}/spark-jars/hadoop-aws-3.3.4.jar,{home}/spark-jars/aws-java-sdk-bundle-1.12.262.jar"

    spark = (
        SparkSession.builder
        .appName("CMS Bronze to Silver")
        .config("spark.jars", jars)
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.access.key", credentials.access_key)
        .config("spark.hadoop.fs.s3a.secret.key", credentials.secret_key)
        .config("spark.hadoop.fs.s3a.endpoint", f"s3.{AWS_REGION}.amazonaws.com")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_bronze(spark):
    """
    Read the raw csv from s3 bronze layer.
    WE set header to True so spark reads the first row as column names.
    We set inferSchema=False because all the coulmns come in as Strings
    from CSV and we will cast types explicitly in the transform step.
    """
    logger.info("Reading bronze data from s3")
    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .option("quote", '"')
        .option("escape", '"')
        .csv(BRONZE_PATH)
    )
    logger.info(f"Bronze row count: {df.count()}")
    return df

def rename_columns(df):
    """
    Rename all columns from CMS abbreviation format to readbale sname_case.
    This is strandardization - downstream users should not need to decode government abbrevation to understand what a column means.
    """
    column_mapping ={
        "Rndrng_NPI":  "provider_npi",
        "Rndrng_Prvdr_Last_Org_Name" : "provider_last_org_name",
        "Rndrng_Prvdr_First_Name" : "provider_first_name",
        "Rndrng_Prvdr_MI" : "provider_middle_initial",
        "Rndrng_Prvdr_Crdntls" : "provider_credentials",
        "Rndrng_Prvdr_Gndr" : "provider_gender",
        "Rndrng_Prvdr_Ent_Cd" : "provider_entity_code",
        "Rndrng_Prvdr_St1" : "provider_street1",
        "Rndrng_Prvdr_St2" : "provider_street2",
        "Rndrng_Prvdr_City" : "provider_city",
        "Rndrng_Prvdr_State_Abrvtn" : "provider_state",
        "Rndrng_Prvdr_State_FIPS" : "provider_state_fips",
        "Rndrng_Prvdr_Zip5" : "provider_zip",
        "Rndrng_Prvdr_RUCA" : "provider_ruca_code",
        "Rndrng_Prvdr_RUCA_Desc" : "provider_ruca_desc",
        "Rndrng_Prvdr_Cntry" : "provider_country",
        "Rndrng_Prvdr_Type" : "provider_type",
        "Rndrng_Prvdr_Mdcr_Prtcptg_Ind" : "provider_medicare_participating",
        "Tot_HCPCS_Cds" : "total_hcpcs_codes",
        "Tot_Benes" : "total_beneficiaries",
        "Tot_Srvcs" : "total_services",
        "Tot_Sbmtd_Chrg" : "total_submitted_charge",
        "Tot_Mdcr_Alowd_Amt" : "total_medicare_allowed_amt",
        "Tot_Mdcr_Pymt_Amt" : "total_medicare_payment_amt",
        "Tot_Mdcr_Stdzd_Amt" : "total_medicare_standardized_amt"
    }

    for old_name, new_name in column_mapping.items():
        df = df.withColumnRenamed(old_name, new_name)
    logger.info("Columns renamed to snake_case.")
    return df

def filter_invalid_rows(df):
    """
    Reomve rows where provider_npi is null or empty.
    the NPI is the unique identifier fr every provider - without it
    a row cannot be tracked, joined, or used for any analysts.
    Keeping null NPI rows would pollute the silver layer with unusable data
    """

    before = df.count()
    df = df.filter(
        F.col("provider_npi").isNotNull() & (F.col("provider_npi") != "")
    )

    after = df.count()
    logger.info(f"Filtered {before - after} rows with null/empty NPI. Remaining : {after}")
    return df

def cast_numeric_columns(df):
    """
    Cast payment and count columns from string to float.
    CSV files store everything as text - we need actual numbers
    to do math, aggregation, and comparisons in the Gold layer.
    We use FLoatTpe for payement amounts as they contain decimals.
    """
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

    logger.info("Numeric columns casted to float.")
    return df

def deduplicate(df):
    """
    Keep only one row per provider NPI.
    Each provider shpuld appear once in the silver layer.
    We keep the first occurance and drop any subsequent duplicates.
    """
    before = df.count()
    df = df.dropDuplicates(["provider_npi"])
    after = df.count()
    logger.info(f"Removed {before - after} duplicate NPI. Remaining: {after}")
    return df

def add_derived_columns(df):
    """
    Add payment_per_services : total medicare Payementdivides by total services.
    This derived metric did not exists in the raw data - we are creating analytical value. It tells you how much Medicare pais
    per individual procedure for each provider, useful for identifiying billing patterns.
    We handle division by zero by returning null total_serivices is 0.
    """

    df = df.withColumn(
        "payment_per_service",
        F.when(
            F.col("total_services") > 0,
            F.col("total_medicare_payment_amt") / F.col("total_services")
        ).otherwise(None)
    )
    logger.info("Derived column payment_per_service added.")
    return df

def write_silver(df):
    """
    Write transformed data to s3 silver as parquet, partitioned by state.
    Parquet is columnar and compressed - much faster and cheaper to query than csv, 
    especially with Athena only scans the relevent state folder when a query filters by state,
    instead of scanning the entire dataset
    """
    logger.info(f"Writing Silver Parquet to {SILVER_PATH}")
    (
        df.write
        .mode("overwrite")
        .partitionBy("provider_state")
        .parquet(SILVER_PATH)
    )
    logger.info("Silver layer written successfully.")

def main():
    spark = create_spark_session()

    #read bronze data
    df = read_bronze(spark)

    #rename column names to snake_case
    df = rename_columns(df)

    #remove the rows with null NPI 
    df = filter_invalid_rows(df)

    # cast string column to proper numeric types
    df = cast_numeric_columns(df)

    # remove duplicate rows based on NPI
    df = deduplicate(df)

    # add derived column payment per service    
    df = add_derived_columns(df)

    # write clean parquet to silver partitioned by state
    write_silver(df)

    logger.info("Bronze to silver transformation completed successfully.")
    spark.stop()

if __name__ == "__main__":
    main()