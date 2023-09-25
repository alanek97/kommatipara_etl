from pyspark.sql import SparkSession
import logging
from logging.handlers import TimedRotatingFileHandler

class SetupKommatiPara():
    def c_spark_session_init(self) -> SparkSession:

        logging.info(f'Spark session is established')

        return SparkSession.builder.appName("ETL KommatiPara").getOrCreate()
    
    def c_spark_session_close(self, spark: SparkSession) -> None:

        logging.info(f'Spark session is terminated')
        
        spark.sparkContext.stop()

    def c_logging_setup(self, job: str):
        logging.basicConfig(
        level=logging.INFO,
        encoding = 'UTF-8',
        format="%(asctime)s | %(process)d | %(levelname)s | %(name)s | %(module)s  | %(funcName)s | %(message)s",
        datefmt='%d-%m-%y %H:%M:%S',
        handlers=[TimedRotatingFileHandler(f'./{job}.log', backupCount=5, when='D'), logging.StreamHandler()])