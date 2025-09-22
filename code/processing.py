import argparse
import logging
import os
import pandas as pd
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import DoubleType

if os.environ.get("IS_TRAINING_JOB", "").lower() == "true":
    from services.HDFSManager import HDFSManager

    PROCESSING_PATH_INPUT = os.environ.get("SM_CHANNEL_INPUT", None)
    PROCESSING_PATH_OUTPUT = os.environ.get("SM_OUTPUT_DIR", "/opt/ml/output/data")
else:
    from scripts.services.HDFSManager import HDFSManager

    BASE_PATH = os.path.join("/", "opt", "ml")
    PROCESSING_PATH = os.path.join(BASE_PATH, "processing")
    PROCESSING_PATH_INPUT = os.path.join(PROCESSING_PATH, "input")
    PROCESSING_PATH_OUTPUT = os.path.join(PROCESSING_PATH, "output")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

## Spark Initializer
#

spark = (
    SparkSession.builder.config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)

logger.info("##################")
sc = spark.sparkContext
logger.info("Spark Configurations:")
logger.info("spark.executor.cores: {}".format(sc._conf.get("spark.executor.cores")))
logger.info("spark.executor.memory: {}".format(sc._conf.get("spark.executor.memory")))
logger.info(
    "spark.executor.memoryOverhead: {}".format(
        sc._conf.get("spark.executor.memoryOverhead")
    )
)
logger.info(
    "spark.executor.instances: {}".format(sc._conf.get("spark.executor.instances"))
)
logger.info("spark.driver.memory: {}".format(sc._conf.get("spark.driver.memory")))
logger.info("spark.driver.cores: {}".format(sc._conf.get("spark.driver.cores")))
logger.info("##################")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    args = parser.parse_args()

    logger.info("Arguments: {}".format(args))

    hdfs_manager = HDFSManager(spark)

    ## Move ProcessingInputs to HDFS
    hdfs_manager.copy_full_to_hdfs(PROCESSING_PATH_INPUT)

    df_e = hdfs_manager.load_df(spark, "energy_dataset")

    columns_to_drop = [
        "generation fossil coal-derived gas",
        "generation fossil oil shale",
        "generation fossil peat",
        "generation geothermal",
        "generation hydro pumped storage aggregated",
        "generation marine",
        "generation wind offshore",
        "forecast wind offshore eday ahead",
        "total load forecast",
        "forecast solar day ahead",
        "forecast wind onshore day ahead",
    ]

    df_e = df_e.drop(*columns_to_drop)

    for column in df_e.schema.names:
        if column != "time":
            df_e = df_e.withColumn(column, df_e[column].cast(DoubleType()))
            df_e = df_e.withColumn(column, F.round(F.col(column), 2))

    df_e.select(
        [
            F.count(F.when(F.isnan(c) | F.col(c).isNull(), c)).alias(c)
            for c in df_e.schema.names
            if c != "time"
        ]
    ).toPandas()

    def interpolate(pdf):
        pdf = pdf.set_index("time")
        pdf.interpolate(
            method="linear", limit_direction="forward", inplace=True, axis=0
        )
        pdf.reset_index(inplace=True)
        return pdf

    df_e_p = df_e.toPandas()
    df_e_p = interpolate(df_e_p)

    pandas_version = pd.__version__

    if str(pandas_version).startswith("2."):
        df_e_p.iteritems = df_e_p.items

    df_e = spark.createDataFrame(df_e_p)

    logger.info("Shape: ({},{})".format(df_e.count(), len(df_e.columns)))

    df_w = hdfs_manager.load_df(spark, "weather_features")

    columns = [
        "city_name",
        "weather_id",
        "weather_main",
        "weather_description",
        "weather_icon",
        "dt_iso",
    ]

    for c in df_w.columns:
        if c not in columns:
            df_w = df_w.withColumn(c, df_w[c].cast(DoubleType()))

    df_w = df_w.withColumn("time", F.to_timestamp("dt_iso", "yyyy-MM-dd HH:mm:ssVV"))

    df_w = df_w.drop("dt_iso")

    ## Remove duplicates

    df_w.distinct().groupby("city_name").count().show()

    df_w = df_w.orderBy("time").coalesce(1).dropDuplicates(subset=["city_name", "time"])

    df_w.distinct().groupby("city_name").count().show()

    df_w_barcelona = df_w.filter(F.col("city_name") == " Barcelona")
    df_w_bilbao = df_w.filter(F.col("city_name") == "Bilbao")
    df_w_madrid = df_w.filter(F.col("city_name") == "Madrid")
    df_w_seville = df_w.filter(F.col("city_name") == "Seville")
    df_w_valencia = df_w.filter(F.col("city_name") == "Valencia")

    df_w_barcelona = df_w_barcelona.select(
        [F.col(c).alias(c + "_barcelona") for c in df_w_barcelona.columns]
    ).drop("city_name_barcelona")
    df_w_bilbao = df_w_bilbao.select(
        [F.col(c).alias(c + "_bilbao") for c in df_w_bilbao.columns]
    ).drop("city_name_bilbao")
    df_w_madrid = df_w_madrid.select(
        [F.col(c).alias(c + "_madrid") for c in df_w_madrid.columns]
    ).drop("city_name_madrid")
    df_w_seville = df_w_seville.select(
        [F.col(c).alias(c + "_seville") for c in df_w_seville.columns]
    ).drop("city_name_seville")
    df_w_valencia = df_w_valencia.select(
        [F.col(c).alias(c + "_valencia") for c in df_w_valencia.columns]
    ).drop("city_name_valencia")

    logger.info("Join energy_dataset_df with weather_features_df")

    df_e = df_e.join(
        df_w_barcelona, df_e.time == df_w_barcelona.time_barcelona, how="full"
    ).drop("time_barcelona")
    df_e = df_e.join(
        df_w_bilbao, df_e.time == df_w_bilbao.time_bilbao, how="full"
    ).drop("time_bilbao")
    df_e = df_e.join(
        df_w_madrid, df_e.time == df_w_madrid.time_madrid, how="full"
    ).drop("time_madrid")
    df_e = df_e.join(
        df_w_seville, df_e.time == df_w_seville.time_seville, how="full"
    ).drop("time_seville")
    df_e = df_e.join(
        df_w_valencia, df_e.time == df_w_valencia.time_valencia, how="full"
    ).drop("time_valencia")

    logger.info(
        "Writing output file {} to {}".format("energy_full.csv", PROCESSING_PATH_OUTPUT)
    )

    # df_e.write.mode("overwrite").parquet("s3://sagemaker-eu-west-1-691148928602/test/energy_full")

    hdfs_manager.save_df(df_e, "energy_full")
    hdfs_manager.copy_from_hdfs(PROCESSING_PATH_OUTPUT, "energy_full")
