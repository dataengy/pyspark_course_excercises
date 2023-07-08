import pyspark
from loguru import logger as log
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


def run_spark():
    spark = SparkSession.builder.master("local") \
        .config('spark.sql.autoBroadcastJoinThreshold', 0) \
        .config('spark.sql.adaptive.enabled', 'false') \
        .getOrCreate()
    display(spark)
    return spark


# def read(tn):


@log.catch
def main():
    global spark
    spark = run_spark()


if __name__ == '__main__': main()
