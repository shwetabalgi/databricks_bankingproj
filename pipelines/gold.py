from pyspark import pipelines as dp
from pyspark.sql.functions import *
from utilities import utils

spark.sql("use catalog banking")
spark.sql("use schema gold")

@dp.table
def customer_profile() :
  customers = dp.read("banking.silver.customers_silver")
  accounts = spark.read.table("banking.silver.accounts_silver")
  loans = spark.read.table("banking.silver.loans_silver")
   
  df_joined = (
    customers.join(accounts, "customer_id", "inner")
    .join(loans, "customer_id", "inner")
    .groupBy("customer_id","name")
    .agg(count("account_id").alias("total_accounts"),
      count("loan_id").alias("total_loans"),
      sum("loan_amount").alias("total_loan_amount"),
      sum("balance").alias("total_balance"))
    .select(
      "customer_id",
      "name",
      "total_accounts",
      "total_balance",
      "total_loans",
      "total_loan_amount"
    )
  )
  return df_joined


@dp.table
def branch_performance() :

  branches = spark.read.table("banking.silver.branches_silver")
  accounts = spark.read.table("banking.silver.accounts_silver")
  transactions = spark.read.table("banking.silver.transactions_silver")
    
  df_joined = (
    branches.join(accounts, "branch_id", "inner")
    .join(transactions, "account_id", how="inner")
    .groupBy("branch_id","branch_name")
    .agg(count("account_id").alias("total_accounts"),
      sum("amount").alias("total_txn_amount"))
    .select(
      "branch_id",
      "branch_name",
      "total_accounts",
      "total_txn_amount"
    )
  )
  return df_joined
  
@dp.table
def customer_transaction_summary () :
  transactions = spark.read.table("banking.silver.transactions_silver")
  accounts = spark.read.table("banking.silver.accounts_silver")
  customers = spark.read.table("banking.silver.customers_silver")
  
  df_joined = (
    transactions.join(accounts, "account_id", "inner")
    .join(customers, "customer_id", "inner")
    .groupBy("customer_id","name")
    .agg(count("txn_id").alias("total_transactions"),
      sum("amount").alias("total_spend"))
    .select(
      "customer_id",
      "name",
      "total_transactions",
      "total_spend"
    )
  )
  return df_joined

@dp.table
def loans_summary () :
  loans = spark.read.table("banking.silver.loans_silver")
  
  df_final = (
    loans.groupBy("loan_type")
    .agg(count("loan_id").alias("total_loans"),
      sum("loan_amount").alias("total_loan_amount"))
    .select(
      "loan_type",
      "total_loans",
      "total_loan_amount"
    )
  )
  return df_final

@dp.table
def account_summary () :
  accounts = spark.read.table("banking.silver.accounts_silver")
  
  df_final = (
    accounts.groupBy("account_type")
    .agg(count("account_id").alias("total_accounts"),
      sum("balance").alias("total_balance"))
    .select(
      "account_type",
      "total_accounts",
      "total_balance"
    )
  )
  return df_final

