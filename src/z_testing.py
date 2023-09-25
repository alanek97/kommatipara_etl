
# from pyspark.sql import SparkSession
# from  ETL_source_code.ETL_kommatiPara import ETLKommatiPara

#test_etl = ETLKommatiPara()


import os

#spark = SparkSession.builder.appName("ETL KommatiPara").getOrCreate()



# batch_path = os.path.dirname(os.path.abspath(__file__))
# customer_path = '/home/alan/kommatiPara/files/data_sets/csv_test_inputs/dataset_one_test.csv'
# transation_path = '/home/alan/kommatiPara/files/data_sets/csv_test_inputs/dataset_two_test.csv'
# country_flags = "'United Kingdom' 'Netherlands'"

# print(f'python3 {batch_path}/Bitcoin_datamart_batch.py -c "{customer_path}" -t "{transation_path}" -f {country_flags}')

# import logging
# from logging.handlers import TimedRotatingFileHandler

# job = 'j_bitcoin_dm'



# logging.basicConfig(
#         level=logging.INFO,
#         encoding = 'UTF-8',
#         format="%(asctime)s | %(process)d | %(levelname)s | %(name)s | %(module)s  | %(funcName)s | %(message)s",
#         datefmt='%d-%m-%y %H:%M:%S',
#         handlers=[TimedRotatingFileHandler(f'./{job}.log', backupCount=5, when='D'), logging.StreamHandler()])

# class klasa_test():
#     def test(self):
#         logging.warning('tedt', extra={'className' : 'test'})


# klasa_test().test()

 
import etl_source_code
import utils

