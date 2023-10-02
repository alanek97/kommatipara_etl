import chispa
import os
from pyspark.sql import SparkSession
import pytest
import shutil
from source.etl_kommati_para import JobKommatiPara
from pyspark.sql.functions import col


test_etl = JobKommatiPara()


def test_initialization_of_spark_after_ETLKommatiPara_class_run():
    sp = SparkSession.getActiveSession()
    assert sp != None


@pytest.fixture()
def df_in():
    return test_etl.spark.createDataFrame([(1, 2., 'string1', 'string 4', 'Y'),
                                           (2, 3., 'string2', 'string 5', 'N'),
                                           (3, 4., 'string3', 'string 6', 'Y'),
                                           (4, 5., 'string4', 'string 7', 'X'),
                                           (5, 6., 'string5', 'string 8', 'N')],
                                          schema='a int, b double, c string, d string, e string')


@pytest.mark.parametrize('columns,type,df_out', [(['a', 'c'], 'selected', test_etl.spark.createDataFrame([(1, 'string1'),
                                                                                                          (2, 'string2'),
                                                                                                          (3, 'string3'),
                                                                                                          (4, 'string4'),
                                                                                                          (5, 'string5')],
                                                                                                         schema='a int, c string')),
                                                 (['a', 'b', 'c'], 'other', test_etl.spark.createDataFrame([('string 4', 'Y'),
                                                                                                            ('string 5', 'N'),
                                                                                                            ('string 6', 'Y'),
                                                                                                            ('string 7', 'X'),
                                                                                                            ('string 8', 'N')],
                                                                                                           schema='d string, e string'))])
def test_selection_of_columns(columns, type, df_in, df_out):
    df_test = test_etl.transform_select_columns(df_in, columns, type=type)
    chispa.assert_df_equality(df_out, df_test, ignore_row_order=True)


@pytest.mark.parametrize('column,criteria,df_out', [(col('e'), ['Y', 'N'], test_etl.spark.createDataFrame([(1, 2., 'string1', 'string 4', 'Y'),
                                                                                                          (2, 3., 'string2', 'string 5', 'N'),
                                                                                                           (3, 4., 'string3', 'string 6', 'Y'),
                                                                                                           (5, 6., 'string5', 'string 8', 'N')],
                                                                                                          schema='a int, b double, c string, d string, e string')),
                                                    (col('a'), [1, 2, 3], test_etl.spark.createDataFrame([(1, 2., 'string1', 'string 4', 'Y'),
                                                                                                          (2, 3., 'string2',
                                                                                                           'string 5', 'N'),
                                                                                                          (3, 4., 'string3', 'string 6', 'Y')],
                                                                                                         schema='a int, b double, c string, d string, e string'))])
def test_filter_data_based_on_list(column, criteria, df_in, df_out):
    df_test = test_etl.transform_filter_isin_source(df_in, column, criteria)
    chispa.assert_df_equality(df_out, df_test, ignore_row_order=True)


def test_renaming_of_columns(df_in):
    df_out = test_etl.spark.createDataFrame([(1, 2., 'string1', 'string 4', 'Y'),
                                            (2, 3., 'string2', 'string 5', 'N'),
                                            (3, 4., 'string3', 'string 6', 'Y'),
                                            (4, 5., 'string4', 'string 7', 'X'),
                                            (5, 6., 'string5', 'string 8', 'N')],
                                            schema='id int, b double, country string, d string, e string')
    df_test = test_etl.transform_rename_columns(
        df_in, {'a': 'id', 'c': 'country'})
    chispa.assert_df_equality(df_out, df_test, ignore_row_order=True)


def test_joing_two_dataframes(df_in):
    df_in2 = test_etl.spark.createDataFrame([(1, 'test_1'),
                                            (2, 'test_2'),
                                            (3, 'test_3')],
                                            schema='id int, desc string')

    df_out = test_etl.spark.createDataFrame([(1, 2., 'string1', 'string 4', 'Y', 1, 'test_1'),
                                            (2, 3., 'string2', 'string 5',
                                             'N', 2, 'test_2'),
                                            (3, 4., 'string3', 'string 6', 'Y', 3, 'test_3')],
                                            schema='a int, b double, c string, d string, e string, id int, desc string')

    df_test = test_etl.transform_join_sources(
        df_in, df_in2, [df_in.a == df_in2.id])
    chispa.assert_df_equality(df_out, df_test, ignore_row_order=True)


def test_export_csv_file(df_in):
    export_path = './tests/data/csv_file_export.csv'
    df_out = df_in.coalesce(1)

    if os.path.exists(export_path):
        shutil.rmtree(export_path)

    test_etl.load_export_dataframe(df_out, export_path)

    assert os.path.exists(export_path)

    if os.path.exists(export_path):
        shutil.rmtree(export_path)


def test_import_csv_file(df_in):
    import_path = './tests/data/dataset_import_test.csv'

    df_test = test_etl.extract_source_csv(import_path)

    chispa.assert_df_equality(df_in, df_test, ignore_row_order=True)


def test_datamart_etl_process():
    path = './tests/data/client_data.csv'

    if os.path.exists(path):
        shutil.rmtree(path)

    test_etl.job_bitcoin_datamart({'customer': "./tests/data/dataset_one_test.csv",
                                   'transactions': "./tests/data/dataset_two_test.csv",
                                   'country_flags': ['United Kingdom', 'Netherlands'],
                                   'location': os.path.dirname(path)})

    df_out = test_etl.spark.createDataFrame([(3, 'mail_3', 'Netherlands',
                                              3, 'string_3', 'string_19'),
                                             (6, 'mail_6', 'Netherlands',
                                              6, 'string_6', 'string_22'),
                                             (8, 'mail_8', 'United Kingdom',
                                              8, 'string_8', 'string_24'),
                                             (10, 'mail_10', 'United Kingdom',
                                              10, 'string_10', 'string_26')],
                                            schema='id int, email string, country string, client_identifier int, bitcoin_address string, credit_card_type string'
                                            )

    df_test = test_etl.extract_source_csv(path)
    chispa.assert_df_equality(df_out, df_test, ignore_row_order=True)

    if os.path.exists(path):
        shutil.rmtree(path)
