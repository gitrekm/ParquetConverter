from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *


if __name__ == "__main__":
    sc = SparkContext(appName="ParquetConverter")
    sqlContext = SQLContext(sc)
    input_bucket="gs://maxcart-dataset/ecom-2019-Nov.csv"
    output_bucket='gs://maxcart-dataset/parquet/ecom-2019-Nov.parquet'
    schema = StructType([
            StructField("event_time", TimestampType(), True),
            StructField("event_type", StringType(), True),
            StructField("product_id", IntegerType(), True),
            StructField("category_id", IntegerType(), True),
            StructField("category_code", StringType(), True),
            StructField("brand", StringType(), True),
            StructField("price", FloatType(), True),
            StructField("user_id", IntegerType(), True),
            StructField("user_session", StringType(), True)])
    
    rdd = sc.textFile(input_bucket).map(lambda line: line.split(","))
    df = sqlContext.createDataFrame([rdd], schema)
    df.write.parquet(output_bucket)