from pyspark.sql import SparkSession
import paramiko
import io
import time

# PostgreSQL JDBC config
POSTGRES_URL = "jdbc:postgresql://w3.training5.modak.com:5432/postgres"
POSTGRES_PROPERTIES = {
    "user": "mt24080",
    "password": "mt24080@m12y24",
    "driver": "org.postgresql.Driver"
}


# SFTP config
SFTP_HOST = 'w3.devcrawlers.modak.com'
SFTP_PORT = 22
SFTP_USERNAME = 'sftp_srv_account'
SFTP_PASSWORD = 'eHK8HAWbQX'
REMOTE_PATH = '/uploads/nye_test'

# Spark session
spark = SparkSession.builder \
    .appName("SFTP Triggered Ingest") \
    .getOrCreate()


def read_dat_as_df(file_obj):
    """
    Reads a streamed .dat file as a Spark DataFrame
    """
    file_bytes = file_obj.read()
    file_str = file_bytes.decode("utf-8")
    file_rdd = spark.sparkContext.parallelize(file_str.strip().splitlines())
    header = file_rdd.first()
    data_rdd = file_rdd.filter(lambda row: row != header).map(lambda row: row.split(","))
    columns = header.split(",")

    return spark.createDataFrame(data_rdd, columns)


def process_files():
    transport = paramiko.Transport((SFTP_HOST, SFTP_PORT))
    transport.connect(username=SFTP_USERNAME, password=SFTP_PASSWORD)
    sftp = paramiko.SFTPClient.from_transport(transport)

    sftp.chdir(REMOTE_PATH)
    files = sftp.listdir()
    trg_files = [f for f in files if f.endswith('.trg')]

    for trg in trg_files:
        base = trg[:-4]
        dat_file = base + '.dat'

        if dat_file in files:
            print(f"Found pair: {trg}, {dat_file}")
            with sftp.open(dat_file, 'r') as file_obj:
                buffered_file = io.BytesIO(file_obj.read())
                df = read_dat_as_df(buffered_file)

                table_name = base.lower()  # or preserve casing depending on your DB
                print(f"Inserting data into PostgreSQL table: {table_name}")

                df.write.jdbc(
                    url=POSTGRES_URL,
                    table=table_name,
                    mode='append',
                    properties=POSTGRES_PROPERTIES
                )
                print(f"Data from {dat_file} inserted into table `{table_name}`.")

            # Optional cleanup:
            # sftp.remove(trg)
            # sftp.remove(dat_file)

    sftp.close()
    transport.close()


if __name__ == "__main__":
    while True:
        process_files()
        time.sleep(60)  # Check every 60 seconds

