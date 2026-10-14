# Databricks notebook source
from pyspark.sql import functions as F

CATALOG = "student_marks"  
SCHEMA = "students_marks"

silver = spark.table(f"{CATALOG}.{SCHEMA}.silver_student_marks")

gold = silver.select(
    F.col("name").alias("student_name"),
    F.col("registration_number").alias("roll_number"),
    F.col("program"),
    F.round(F.col("average_marks"), 2).alias("average_marks")
)

(
    gold.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(f"{CATALOG}.{SCHEMA}.gold_student_results")
)

display(spark.table(f"{CATALOG}.{SCHEMA}.gold_student_results"))