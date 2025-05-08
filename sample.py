from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, sum, count, when, row_number
from pyspark.sql.window import Window

# Initialize Spark session
spark = SparkSession.builder \
    .appName("StudentDataProcessing_ADLS") \
    .getOrCreate()

# ADLS Gen2 Config
storage_account = "trainingnew"
container = "training-2024"
gen2_account_key = "HyBbfPSMNq+XHMyU6w+DyDFMMRfb1ERLg8L81BxYqlHri1wUdFvLVqGXhJaMHbgOxt2T21KQuxw0+AStt8bs/g=="

# Set Spark config for ADLS Gen2
spark.conf.set(f"fs.azure.account.key.{storage_account}.dfs.core.windows.net", gen2_account_key)

# CSV file path in ADLS Gen2
data_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net/mt24080_nabu/school_*.csv"

# Read CSV file from ADLS
df = spark.read.csv(data_path, header=True, inferSchema=True)

# List of subjects
subject_cols = ["Math", "English", "Hindi", "Science", "Economics", 
                "History", "Geography", "Telugu", "Physics", "Chemistry"]

# Fill missing values with 0
df = df.fillna(0, subset=subject_cols)

# Compute Total Marks & Percentage
df = df.withColumn("Total_Marks", sum(col(subj) for subj in subject_cols))

# Count of subjects attempted
df = df.withColumn("Subject_Count", sum(when(col(subj).isNotNull(), 1).otherwise(0) for subj in subject_cols))

# Calculate percentage
df = df.withColumn("Percentage", (col("Total_Marks") / (col("Subject_Count") * 100)) * 100)

# Assign grades
df = df.withColumn("Grade",
    when(col("Percentage") >= 90, "A+")
    .when(col("Percentage") >= 80, "A")
    .when(col("Percentage") >= 70, "B")
    .when(col("Percentage") >= 60, "C")
    .otherwise("D")
)

# Rank within each section
window_spec = Window.partitionBy("Class", "Section").orderBy(col("Total_Marks").desc())
df = df.withColumn("Rank", row_number().over(window_spec))

# Class averages
class_avg_df = df.groupBy("Class").agg(
    *[avg(col(subj)).alias(f"Avg_{subj}") for subj in subject_cols],
    avg("Percentage").alias("Class_Percentage")
)

# School-level stats
school_monitoring_df = df.groupBy().agg(
    *[avg(col(subj)).alias(f"School_Avg_{subj}") for subj in subject_cols]
)

# Class toppers
window_class_topper = Window.partitionBy("Class").orderBy(col("Percentage").desc())
topper_df = df.withColumn("Rank", row_number().over(window_class_topper)).filter(col("Rank") == 1)
school_monitoring_df = school_monitoring_df.join(topper_df.select("Class", "Name", "Percentage"), "Class", "left")

# PostgreSQL connection details
pg_host = "w3.training5.modak.com"
pg_db = "postgres"
pg_user = "mt24080"
pg_password = "mt24080@m12y24"
pg_url = f"jdbc:postgresql://{pg_host}:5432/{pg_db}"
pg_properties = {
    "user": pg_user,
    "password": pg_password,
    "driver": "org.postgresql.Driver"
}

# Write DataFrames to PostgreSQL
df.write.jdbc(url=pg_url, table="transformed_student_performance", mode="overwrite", properties=pg_properties)
class_avg_df.write.jdbc(url=pg_url, table="transformed_class_avg", mode="overwrite", properties=pg_properties)
school_monitoring_df.write.jdbc(url=pg_url, table="transformed_school_monitoring", mode="overwrite", properties=pg_properties)
