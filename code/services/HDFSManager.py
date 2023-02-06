import logging
import os
from subprocess import PIPE, Popen
import traceback

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

## HDFS Manager
#
class HDFSManager:
    def __init__(self, spark):
        sc = spark.sparkContext
        self.URI = sc._gateway.jvm.java.net.URI
        self.Path = sc._gateway.jvm.org.apache.hadoop.fs.Path
        self.FileSystem = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem
        self.Configuration = sc._gateway.jvm.org.apache.hadoop.conf.Configuration

        self.fs = sc._jvm.org \
            .apache.hadoop \
            .fs.FileSystem \
            .get(sc._jsc.hadoopConfiguration())

        self.hdfs_path = spark.sparkContext._jsc.hadoopConfiguration().get("fs.defaultFS")
        
        logger.info("Creating /tmp folder in HDFS")
        
        put = Popen(["hdfs", "dfs", "-mkdir", "-p", os.path.join(self.hdfs_path, "tmp")], stdin=PIPE, bufsize=-1)
        put.communicate()
        
        
    def save_df(self, df, file, separator=",", header=True, quote='"'):
        try:
            logger.info("Saving {} in path {}".format(file, os.path.join(self.hdfs_path, "tmp", file)))
            df.repartition(1).write \
                .format("com.databricks.spark.csv") \
                .mode('overwrite') \
                .option("quote", quote) \
                .option("header", header) \
                .option("sep", separator) \
                .csv("{}".format(os.path.join(self.hdfs_path, "tmp", file)))

        except Exception as e:
            stacktrace = traceback.format_exc()

            logger.error(stacktrace)

            raise e
            
    def load_df(self, spark_session, file, separator=","):
        try:
            logger.info("Loading {} from path {}".format(file, os.path.join(self.hdfs_path, "tmp", file)))
            
            df = spark_session.read \
                .option("header", True) \
                .option("sep", separator) \
                .csv("{}".format(os.path.join(self.hdfs_path, "tmp", file)))

            return df
        except Exception as e:
            stacktrace = traceback.format_exc()

            logger.error(stacktrace)

            raise e
    
    ## This method allows you to copy files from local file system to HDFS
    #
    def copy_full_to_hdfs(self, path):
        try:
            logger.info("EBS files: {}".format(os.listdir(path)))
            
            if os.path.isdir(path):
                files = [f for f in os.listdir(path)]

                for file in files:
                    logger.info("Copy {} in HDFS {}".format(os.path.join(path, file), os.path.join(self.hdfs_path, "tmp", file)))

                    put = Popen(["hdfs", "dfs", "-put", os.path.join(path, file), os.path.join(self.hdfs_path, "tmp", file)], stdin=PIPE, bufsize=-1)
                    put.communicate()                       
        except Exception as e:
            stacktrace = traceback.format_exc()

            logger.error(stacktrace)

            raise e
            
    ## This method allows you to copy a selected file from local file system to HDFS
    #
    def copy_to_hdfs(self, path, file):
        try:
            logger.info("Copy file from {} to HDFS".format(os.path.join(path, file)))
            
            put = Popen(["hdfs", "dfs", "-put", os.path.join(path, file), os.path.join(self.hdfs_path, "tmp", file)], stdin=PIPE, bufsize=-1)
            put.communicate()                  
        except Exception as e:
            stacktrace = traceback.format_exc()

            logger.error(stacktrace)

            raise e
            
    ## This method allows you to copy a selected file from HDFS to local file system
    #
    def copy_from_hdfs(self, path, file):
        try:
            logger.info("Copy file from HDFS to {}".format(os.path.join(path, file)))
            
            put = Popen(["hdfs", "dfs", "-copyToLocal", os.path.join(self.hdfs_path, "tmp", file), os.path.join(path, file)], stdin=PIPE, bufsize=-1)
            put.communicate()                  
        except Exception as e:
            stacktrace = traceback.format_exc()

            logger.error(stacktrace)

            raise e