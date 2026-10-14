# Databricks notebook source
from pyspark.sql import functions as F

CATALOG = "student_marks"  # Change this to your catalog if needed
SCHEMA = "students_marks"
#INPUT_DIR = f"/Volumes/{CATALOG}/{SCHEMA}/student_demo/input"
INPUT_DIR = "/Volumes/student_marks/students_marks/source_files"

def save_as_delta(df, table_name):
    (
        df.write
          .format("delta")
          .mode("overwrite")
          .saveAsTable(f"{CATALOG}.{SCHEMA}.{table_name}")
    )

# Student information
students = (
    spark.read
         .option("header", True)
         .option("inferSchema", False)
         .csv(f"{INPUT_DIR}/students.csv")
         .select(
             F.col("registration_number").cast("string"),
             F.col("name").cast("string"),
             F.col("email").cast("string"),
             F.col("phone_number").cast("string"),
             F.col("program").cast("string")
         )
)

save_as_delta(students, "bronze_students")

# Marks from the three tests
for test_number in (1, 2, 3):
    test_df = (
        spark.read
             .option("header", True)
             .option("inferSchema", False)
             .csv(f"{INPUT_DIR}/test{test_number}.csv")
             .select(
                 F.col("registration_number").cast("string"),
                 F.col("marks").cast("double").alias(f"test{test_number}_marks")
             )
    )

    save_as_delta(test_df, f"bronze_test{test_number}")

print("Bronze tables created.")