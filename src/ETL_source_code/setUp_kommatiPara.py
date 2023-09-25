from pyspark.sql import SparkSession
import logging

class setUpKommatiPara():
    def c_spark_session_init(self) -> SparkSession:
        return SparkSession.builder.appName("ETL KommatiPara").getOrCreate()
    
    def c_spark_session_close(self, spark: SparkSession) -> None:
        spark.sparkContext.stop()


    def c_logging_setup(self):
        None