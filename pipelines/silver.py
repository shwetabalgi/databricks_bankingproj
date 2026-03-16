from pyspark import pipelines as dp
# from utilities import utils
from pyspark.sql.functions import *

spark.sql("use catalog banking")
spark.sql("use schema silver")

@dp.table
def branches_silver():
    df = (spark.read.format("csv") \
        .option("inferSchema", True) \
        .option("header", True) \
        .load("/Volumes/banking/bronze/csv_files/banking_branches.csv") \
        .withColumn("bronze_timestamp", current_timestamp())
    )
    return df

@dp.table
@dp.expect("non negative balance", "balance >=0")
def accounts_silver() :
    df = (
        spark.read.format("csv")
        .option("inferSchema",True)
        .option("header",True)
        .load("/Volumes/banking/bronze/csv_files/banking_accounts.csv")
        .withColumn("bronze_timestamp",current_timestamp())
        .filter(col("account_type").isNotNull())
    )
    return df

@dp.table
@dp.expect("valid customers", "customer_id is not null")
def customers_silver() :
    df = (
        spark.read.format("csv")
        .option("inferSchema",True)
        .option("header",True)
        .load("/Volumes/banking/bronze/csv_files/banking_customers.csv")
        .withColumn("bronze_timestamp",current_timestamp())
        .withColumn("city", coalesce(col("city"),lit("Unknown")))
    )
    return df

@dp.table
def loans_silver() :
    df = (
        spark.read.format("csv")
        .option("inferSchema",True)
        .option("header",True)
        .load("/Volumes/banking/bronze/csv_files/banking_loans.csv")
        .withColumn("bronze_timestamp",current_timestamp())
        .withColumn("loan_type", coalesce(col("loan_type"),lit("Unknown")))
    )
    return df

@dp.table
@dp.expect("valid amount","amount is not null")
def transactions_silver() :
    df = (
        spark.read.format("csv")
        .option("inferSchema",True)
        .option("header",True)
        .load("/Volumes/banking/bronze/csv_files/banking_transactions.csv")
        .withColumn("bronze_timestamp",current_timestamp())
        .dropDuplicates()
        .filter(col("amount").isNotNull())
    )
    return df

