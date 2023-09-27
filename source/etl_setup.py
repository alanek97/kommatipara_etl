import logging
from logging.handlers import TimedRotatingFileHandler
from pyspark.sql import SparkSession


class SetupKommatiPara():
    def c_spark_session_init(self) -> SparkSession:
        '''
        Type of method: control

        This function establishes new or get existing Spark session.

        input: None
        output: SparkSession
        '''
        logging.info(f'Spark session is established')

        return SparkSession.builder.appName("ETL KommatiPara").getOrCreate()

    def c_spark_session_close(self, spark: SparkSession) -> None:
        '''
        Type of method: control

        This function terminates existing Spark session.

        input: SparkSession
        output: None
        '''
        logging.info(f'Spark session is terminated')

        spark.sparkContext.stop()

    def c_logging_setup(self, job: str) -> None:
        '''
        Type of method: control

        This function sets up logging configuration.

        input: [job] str
        output: None
        '''
        logging.basicConfig(
            level=logging.INFO,
            encoding='UTF-8',
            format="%(asctime)s | %(process)d | %(levelname)s | %(name)s | %(module)s  | %(funcName)s | %(message)s",
            datefmt='%d-%m-%y %H:%M:%S',
            handlers=[TimedRotatingFileHandler(f'./{job}.log', backupCount=5, when='d'), logging.StreamHandler()])
