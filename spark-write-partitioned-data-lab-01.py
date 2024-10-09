import argparse
import traceback
from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from pyspark.sql.functions import to_date, col
from pyspark.sql.types import StringType, StructField, StructType

"""
Syntax example:
    Local >> spark-submit --driver-memory 2g --executor-memory 2g spark-write-partitioned-data-lab-01.py --input_path "../notebooks/datasets/parquet/" --output_path "datasets/yellow_taxis_daily/"
    AWS CLI: 
        aws emr add-steps --profile pp-profile --cluster-id j-3EX94GUZ4OK8I \
          --steps Type\=Spark,Name\=Spark_Job_1,ActionOnFailure\=CONTINUE,Args\=\[s3\://aws-pp-dev-labs-carcarre-bucket-143176-001/source/spark-jobs/emr/spark-write-partitioned-data-lab-01.py,--input_path,s3\://aws-pp-dev-labs-carcarre-bucket-143176-001/datasets/yellow-taxis/parquet/tbl_yellow_tripdata_001/,--output_path,s3\://aws-pp-dev-labs-carcarre-bucket-143176-001/datasets/yellow-taxis/parquet/tbl_yellow_tripdata_partitioned_001/]
"""
def parse_args():
    """
    Parse command-line arguments.

    Returns:
        argparse.Namespace: An object containing the parsed arguments.
    """
    parser = argparse.ArgumentParser(description="Input parameters to run this Apache Spark job")

    # Validate input arguments, required to run this job
    parser.add_argument("-i",  "--input_path",  dest="input_path", required=True, help="Enter path with Taxi Raw Parquet Data")
    parser.add_argument("-o",  "--output_path",  dest="output_path", required=True, help="Enter output path where Taxi partitioned Parquet Data will be written")

    # parse and return args
    args = parser.parse_args()
    return args

if __name__ == "__main__":
    # Validate arguments
    args = parse_args()

    # Create SparkSession
    sc = SparkContext()
    spark = SparkSession.builder.appName("spark-job-001").getOrCreate()

    # Enable logging
    log4jLogger = sc._jvm.org.apache.log4j
    logger = log4jLogger.LogManager.getLogger(__name__)

    # Read taxi data, hard-coding paths.
    local_files = args.input_path
    df = spark.read.parquet(local_files)

    # Add Rate Code Name
    data = [("0", "No rate entered"),("1", "Standard rate"), ("2", "JFK"), ("3", "Newark"),("4","Nassau or Westchester "),("5","Negotiated fare"), ("6","Group ride")]

    # Define schema, to ensure data types
    schema = StructType([ \
        StructField("RatecodeID",StringType(),True), \
        StructField("RatecodeName",StringType(),True)
        ])

    try:
        # logging section
        logger.info('creating new Rate Codes dataframe')

        # Create Dataframe for Rate Codes
        df_rate_codes = spark.createDataFrame(data=data,schema=schema)

        # Users do not want to see NULL values for Rate Codes
        df_na_rate_codes = df.na.fill(value=0,subset=["RatecodeID"])

        # logging section
        logger.info('joining Yellow Taxi data with new Rate Codes dataframe')

        # Left join (in any case, there's no more NULL rate ids)
        df = df.join(df_rate_codes, df["RatecodeID"] == df_rate_codes["RatecodeID"], "left").drop(df_rate_codes["RatecodeID"])

        # logging section
        logger.info('creating new partition key(s)')

        # Create new partition key
        df_sink = df.withColumn("p_date",to_date(col('tpep_pickup_datetime')))

        # logging section
        logger.info('writing data to storage; e.g. Amazon S3, HDFS, local fs, etc/')

        # Write to local storage, if not done already:
        df_sink.write.partitionBy("p_date").mode("overwrite").parquet(args.output_path)

        # Stop the session
        spark.stop()

    except Exception as e:
        logger.error('Error while processing the Taxi data: {}'.format(e))
        traceback.print_exc()
