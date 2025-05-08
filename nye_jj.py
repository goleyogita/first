import subprocess
import sys

# Install paramiko if not already installed
try:
    import paramiko
except ImportError:
    subprocess.check_call([sys.executable, "-m", "pip", "install", "paramiko"])
    import paramiko

from pyspark.sql import SparkSession
import io

# Initialize Spark session
spark = SparkSession.builder \
    .appName("SFTP to ADLS Transfer") \
    .getOrCreate()

# SFTP configuration
sftp_host = 'w3.devcrawlers.modak.com'
sftp_port = 22
sftp_user = 'sftp_srv_account'
sftp_password = 'eHK8HAWbQX'
remote_dir = '/uploads/nye_test'

# -------- ADLS Gen2 Configuration --------
storage_account = 'trainingnew'
container = 'training-2024'
gen2_account_key = 'HyBbfPSMNq+XHMyU6w+DyDFMMRfb1ERLg8L81BxYqlHri1wUdFvLVqGXhJaMHbgOxt2T21KQuxw0+AStt8bs/g=='

# Set Spark config for ADLS Gen2
spark.conf.set(f"fs.azure.account.key.{storage_account}.dfs.core.windows.net", gen2_account_key)

# Output path on ADLS
adls_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net/output/"

# -------- Connect to SFTP --------
transport = paramiko.Transport((sftp_host, sftp_port))
transport.connect(username=sftp_user, password=sftp_password)
sftp = paramiko.SFTPClient.from_transport(transport)

# List files in remote directory
files = sftp.listdir(remote_dir)

# Match .csv and .txt files by base name
csv_basenames = {f[:-4] for f in files if f.endswith('.csv')}
txt_basenames = {f[:-4] for f in files if f.endswith('.txt')}
matched_basenames = csv_basenames.intersection(txt_basenames)

# Process each matched .csv file
for base in matched_basenames:
    csv_remote_path = f"{remote_dir}/{base}.csv"

    with sftp.file(csv_remote_path, mode='r') as remote_file:
        csv_content = remote_file.read().decode('utf-8')

    # Convert to RDD
    lines = csv_content.splitlines()
    rdd = spark.sparkContext.parallelize(lines)
    header = rdd.first()
    data_rdd = rdd.filter(lambda row: row != header)

    # Read CSV into DataFrame
    df = spark.read.csv(data_rdd, header=True, inferSchema=True)

    # Write to ADLS
    df.write.mode("overwrite").parquet(f"{adls_path}{base}/")

# Clean up
sftp.close()
transport.close()
spark.stop()
