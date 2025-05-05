from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, sum, count, when, row_number
from pyspark.sql.window import Window


gcs_service_account_key = "/home/goleyogita/Downloads/gcs_cred.json"
# Initialize Spark session
spark = SparkSession.builder.appName("StudentDataProcessing").getOrCreate()
  
spark.conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
spark.conf.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
spark.conf.set("fs.gs.auth.service.account.email","training@modakanalytics.com")
spark.conf.set("fs.gs.auth.service.account.private.key.id","7103e448ac3f01038f663263ebb61e00c904ad8d")
 
spark.conf.set("fs.gs.auth.service.account.private.key","-----BEGIN PRIVATE KEY-----\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQC5HIrqlVlGgmBa\nI60F4Zr8vwV/J5nw10+9VixWeNwcwnAiAXfVVpshGDHWcXiuC/hBO+JAm7ELvLFO\n50ZMMT/W9srKtVov6GVv+85sMm49bsf/5bEGMsySg+d9PRYfRQV3YaicrWln9j1n\nwsTgbIjBZNeL1jefKqIOQKT16ZSlIbDqnMI8lQh4siycvx9pk8T6ulrezO3/GWyf\nWem0awSW/5L8g/mdFMvUZZ2Zuq+g8btdtEFM1l4KrKN3LH1LqCV75mCNa5o61YO9\nLpzMS98ScObwW0htEtdugu0dFDfAIGFyZsvMJ9tcVoaORVzMX3Q7SeeIkVH8lGpv\nQyUQCvi3AgMBAAECggEALIu01kzIZhJb03VAXujynh3t2bKS9gUqWDrTgp+lAHq9\n90D5MGqd/DHRVHSMAP46aKBbiwasbVGkLYO0RAOaBEXxeSR5Jow7VN54x0q/gqMV\nF/yTRBWXay6410eK+k1mylrC86k7c1wrXLvfGs6jfj6hLDKJwhDFXL6rs83ZCn+O\njFsH4njLbRA/8+1IHp4qQrEIcqegDcij/PVXOGtlvdhUOblgk5Kzm9A4Dko3nyu+\nXy3vXMKUjtkgWyIhxhHdaVz5gJMBtWnLkCppm4B+Jzf8bO0Dri+rMhfQyYPSPI6X\niiZNWkFynTwNTT1qR0Xnxlz8UtfVZ1msYpIv7oXPWQKBgQD1EU2Jws87ebXjBipQ\nE1SVpAGGGkm99enIMb3GL6/Ya5NWbTAh/QBVz+srSzU2VLskDmc4JFI+p0RRHGXd\nXkGcg5xrFaAVIk/UF10eiuGygcHNEdwX3WlfEHCd5km9tOwDLaocEdgnflcUXroo\nau03diNDEgunY5vmhA4kJPhM2wKBgQDBXoj0gLWFeo0cq6WkXx8MANklvVICKB0X\nqv7HmUVv1pdt/IdgApbigYjxsJOQngSq9+auorKfgMsLG1DYL3CClj1Imdu/ZT6G\nJG5NFtXyMstA8ReJVR9gJxerBMkVtKPucr317Gx8EMtQueV/IrSP18HP88CBK28M\nTH6LSYycVQKBgFfi96qy+Xy73lXnbR9Af3IW2hEMtmtwmIGaDRPZIDf+BF3XVI6r\n3AXqRc1F8HRmmKKKo8vHgtNDS0XHaGSmG+OUc3EX1Uwe/P/zzQpaBiztSeJQSF0q\no1JbY/fMkZ9+FbHiG4Jrh9hJ/9KnUh2SkzXzoRu5igJiv5NAwo2F0KJHAoGASDGX\nSFHVc6QxkwayrQ+mc8DNUb3BJHT9h5ybysF6nyqrFrE1ia7tzls2WaXnMhMNAxfS\n1FiB//MB23+zS0NK6jZVYwmudWLDWSm41Kc18Vrtb62Tb/6L3EY8G+mJecUbfybv\nrSU7y4YpolYFTNYUO6/9+Dm1IYSpzRmyFSa8jJECgYEA3u1YUTgWBJh8S3IJLPN7\nJcCz7RWxpMDyDGFTOdKKqxjateCrxEB5jf2wo5XIswrbVlKnv83nNSFtv54EX0Mq\n+NNOp79COb1GCcusy5ZNy7T8IsIfyr9EAHHDkHWQP511vvMVAyQ/J2UGZwwEamQ+\nXlJrxOAg9wM9k5TlPF+U8L8=\n-----END PRIVATE KEY-----\n")
 
spark.conf.set("credentials","Training@123$")

# Google Cloud Storage (GCS) Bucket details
gcs_bucket = "modak-training-bucket1"
data_path = f"gs://{gcs_bucket}/mt24080_nabu/school_2_students.csv"
#gcs_service_account_key = "/home/goleyogita/Downloads/gcs_cred.json"

# Set GCS configurations
#spark.conf.set("spark.hadoop.fs.gs.auth.service.account.json.keyfile", gcs_service_account_key)

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
pg_host = "w3.training5.modak.com"  # Replace with Cloud SQL Public IP
pg_db = "postgres"
pg_user = "mt24080"
pg_password = "mt24080@m12y24"
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






