# Databricks notebook source
from pyspark.sql import functions as F
from functools import reduce

CATALOG = "student_marks"  
SCHEMA = "students_marks"

students = spark.table(f"{CATALOG}.{SCHEMA}.bronze_students")
test1 = spark.table(f"{CATALOG}.{SCHEMA}.bronze_test1")
test2 = spark.table(f"{CATALOG}.{SCHEMA}.bronze_test2")
test3 = spark.table(f"{CATALOG}.{SCHEMA}.bronze_test3")

combined = (
    students
    .join(test1, "registration_number", "left")
    .join(test2, "registration_number", "left")
    .join(test3, "registration_number", "left")
)

mark_columns = ["test1_marks", "test2_marks", "test3_marks"]

available_count = reduce(
    lambda a, b: a + b,
    [F.when(F.col(c).isNotNull(), 1).otherwise(0) for c in mark_columns],
    F.lit(0)
)

available_sum = reduce(
    lambda a, b: a + b,
    [F.coalesce(F.col(c), F.lit(0.0)) for c in mark_columns],
    F.lit(0.0)
)

with_average = combined.withColumn(
    "available_average",
    F.when(available_count > 0, available_sum / available_count)
)

# Fill each missing mark with the average of the marks available for that student.
for c in mark_columns:
    with_average = with_average.withColumn(
        f"imputed_{c}",
        F.coalesce(F.col(c), F.col("available_average"))
    )

silver = (
    with_average
    .withColumn(
        "average_marks",
        (
            F.col("imputed_test1_marks")
            + F.col("imputed_test2_marks")
            + F.col("imputed_test3_marks")
        ) / F.lit(3.0)
    )
    .drop("available_average")
)

(
    silver.write
          .format("delta")
          .mode("overwrite")
          .saveAsTable(f"{CATALOG}.{SCHEMA}.silver_student_marks")
)

display(spark.table(f"{CATALOG}.{SCHEMA}.silver_student_marks"))