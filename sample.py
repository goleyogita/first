from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, sum, count, when, row_number
from pyspark.sql.window import Window

# Initialize Spark session
spark = SparkSession.builder.appName("StudentDataProcessing").getOrCreate()

# Google Cloud Storage (GCS) Bucket details
gcs_bucket = "modak-training-bucket1"
data_path = f"gs://{gcs_bucket}/mt24080/school_*.csv"
gcs_service_account_key = "/home/goleyogita/Downloads/gcs_cred.json"
gs://modak-training-bucket1/mt24080_nabu/school_*.csv"
# Set GCS configurations
spark.conf.set("spark.hadoop.fs.gs.auth.service.account.json.keyfile", gcs_service_account_key)

# Read CSV files from GCS
df = spark.read.csv(data_path, header=True, inferSchema=True)

# List of subjects
subject_cols = ["Math", "English", "Hindi", "Science", "Economics", 
                "History", "Geography", "Telugu", "Physics", "Chemistry"]

# Fill missing values with 0
df = df.fillna(0, subset=subject_cols)

# Compute Total Marks & Percentage
df = df.withColumn("Total_Marks", sum(col(subj) for subj in subject_cols))

# Compute Count of Non-Null Subjects
df = df.withColumn("Subject_Count", sum(when(col(subj).isNotNull(), 1).otherwise(0) for subj in subject_cols))

# Compute Percentage (Divide by actual number of subjects attempted)
df = df.withColumn("Percentage", (col("Total_Marks") / (col("Subject_Count") * 100)) * 100)

# Assign Grades
df = df.withColumn("Grade",
    when(col("Percentage") >= 90, "A+")
    .when(col("Percentage") >= 80, "A")
    .when(col("Percentage") >= 70, "B")
    .when(col("Percentage") >= 60, "C")
    .otherwise("D")
)

# Assign Rank within each Section
window_spec = Window.partitionBy("Class", "Section").orderBy(col("Total_Marks").desc())
df = df.withColumn("Rank", row_number().over(window_spec))

df.show()

# Compute Class Averages
class_avg_df = df.groupBy("Class").agg(
    *[avg(col(subj)).alias(f"Avg_{subj}") for subj in subject_cols],
    avg("Percentage").alias("Class_Percentage")
)
class_avg_df.show()

# Compute School Monitoring Data
school_monitoring_df = df.groupBy().agg(
    *[avg(col(subj)).alias(f"School_Avg_{subj}") for subj in subject_cols]
)

# Identify the topper of each class
window_class_topper = Window.partitionBy("Class").orderBy(col("Percentage").desc())
topper_df = df.withColumn("Rank", row_number().over(window_class_topper)).filter(col("Rank") == 1)
school_monitoring_df = school_monitoring_df.join(topper_df.select("Class", "Name", "Percentage"), "Class", "left")
school_monitoring_df.show()

# PostgreSQL connection details (Cloud SQL)
pg_host = "your-cloud-sql-ip"  # Replace with Cloud SQL Public IP
pg_db = "postgres"
pg_user = "your-username"
pg_password = "your-password"
pg_url = f"jdbc:postgresql://{pg_host}:5432/{pg_db}"

pg_properties = {
    "user": pg_user,
    "password": pg_password,
    "driver": "org.postgresql.Driver"
}

# Write tables to PostgreSQL (Cloud SQL)
df.write.jdbc(url=pg_url, table="transformed_student_performance", mode="overwrite", properties=pg_properties)
class_avg_df.write.jdbc(url=pg_url, table="transformed_class_avg", mode="overwrite", properties=pg_properties)
school_monitoring_df.write.jdbc(url=pg_url, table="transformed_school_monitoring", mode="overwrite", properties=pg_properties)






