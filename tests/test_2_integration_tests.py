import chispa
import os
import pytest
import shutil
from pyspark.sql import SparkSession
from source.etl_kommati_para import JobKommatiPara

test_etl = JobKommatiPara()


@pytest.fixture()
def df_out():
    return test_etl.spark.createDataFrame([(3, 'mail_3', 'Netherlands',
                                            3, 'string_3', 'string_19'),
                                           (6, 'mail_6', 'Netherlands',
                                            6, 'string_6', 'string_22'),
                                           (8, 'mail_8', 'United Kingdom',
                                            8, 'string_8', 'string_24'),
                                           (10, 'mail_10', 'United Kingdom',
                                            10, 'string_10', 'string_26')],
                                          schema='id int, email string, country string, client_identifier int, bitcoin_address string, credit_card_type string'
                                          )


@pytest.mark.parametrize('path,options', [('./client_data.csv', ''),
                                          ('./client_data.csv', '-d --debug'),
                                          ('./tests/data/client_data.csv', '-l "./tests/data"')])
def test_execution_by_command_line(path, options, df_out):
    if os.path.exists(path):
        shutil.rmtree(path)

    batch_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    customer_path = './tests/data/dataset_one_test.csv'
    transaction_path = './tests/data/dataset_two_test.csv'
    country_flags = "'United Kingdom' 'Netherlands'"

    os.system(
        f'python3 {batch_path}/source/__main__.py -c "{customer_path}" -t "{transaction_path}" -f {country_flags} ' + options)

    df_test = test_etl.extract_source_csv(path)
    chispa.assert_df_equality(df_out, df_test, ignore_row_order=True)

    if os.path.exists(path):
        shutil.rmtree(path)


def test_spark_close_connection():
    test_etl.control_spark_session_close(test_etl.spark)
    sp = SparkSession.getActiveSession()
    assert sp == None
