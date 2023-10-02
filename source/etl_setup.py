from enum import Enum
import logging
from logging.handlers import TimedRotatingFileHandler
from pyspark.sql import SparkSession

class LoggingLevels(Enum):
    info = logging.INFO
    debug = logging.DEBUG

class SetupKommatiPara():
    def control_spark_session_init(self) -> SparkSession:
        """
        This function establishes new or get existing Spark session.

        input: None
        output: SparkSession
        """
        logging.info(f'Spark session is established')

        return SparkSession.builder.appName("ETL KommatiPara").getOrCreate()

    def control_spark_session_close(self, spark: SparkSession) -> None:
        """
        This function terminates existing Spark session.

        input: SparkSession
        output: None
        """
        logging.info(f'Spark session is terminated')

        spark.sparkContext.stop()

    def control_logging_setup(self, job: str, log_level: str) -> None:
        """
        This function sets up logging configuration.

        input: [level] str,
                [job] str
        output: None
        """
        logging.basicConfig(
            level=LoggingLevels[log_level].value,
            encoding='UTF-8',
            format="%(asctime)s | %(process)d | %(levelname)s | %(name)s | %(module)s  | %(funcName)s | %(message)s",
            datefmt='%d-%m-%y %H:%M:%S',
            handlers=[TimedRotatingFileHandler(f'./{job}.log', backupCount=5, when='d'), logging.StreamHandler()])
