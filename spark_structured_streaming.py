from os import truncate
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.functions import to_date, col, year


BOOTSTRAP_SERVER = "localhost:29092"
TOPIC_NAME = "data_pengguna"

spark = SparkSession \
    .builder \
    .appName("StructuredStreamingContoh") \
    .getOrCreate()

spark.sparkContext.setLogLevel('WARN')

df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", BOOTSTRAP_SERVER) \
    .option("subscribe", TOPIC_NAME) \
    .option("startingOffsets", "earliest") \
    .load()


def foreach_batch_function(data, epoch_id):
    data.show(truncate=False)

df \
    .withColumn("value", f.col("value").cast("STRING")) \
    .withColumn("tanggal_lahir", f.get_json_object(f.col("value"), "$.tanggal_lahir").cast("date")) \
    .withColumn("tahun_lahir", f.year(f.col("tanggal_lahir"))) \
    .withColumn("timestamp", f.current_timestamp()) \
    .withWatermark("timestamp", "1 minutes") \
    .groupBy(f.window(f.col("timestamp"), "10 seconds", "5 seconds"), f.col("tahun_lahir")).agg(f.count(f.col("tahun_lahir")).alias("jumlah")) \
    .select("tahun_lahir", "jumlah") \
    .writeStream \
    .foreachBatch(foreach_batch_function) \
    .start() \
    .awaitTermination()