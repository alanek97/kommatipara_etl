
from etl_source_code.etl_kommati_para import ETLKommatiPara
from pyspark.sql import SparkSession
import pytest
import chispa
import os
import shutil


test_etl = ETLKommatiPara()

def test_spark_connetion():
    sp = SparkSession.getActiveSession()
    assert sp != None

def test_t_select_columns_1():
    df_in = test_etl.spark.createDataFrame([(1, 2., 'string1', 'string 4'),
                                            (2, 3., 'string2', 'string 5'),
                                            (3, 4., 'string3', 'string 6')], 
                                            schema = 'a long, b double, c string, d string')

    df_out = test_etl.spark.createDataFrame([(1, 'string1'),
                                             (2, 'string2'),
                                             (3, 'string3')], 
                                             schema = 'a long, c string')

    df_test = test_etl.t_select_columns(df_in,['a', 'c'])
    chispa.assert_df_equality(df_out, df_test)

def test_t_select_columns_2():
    df_in = test_etl.spark.createDataFrame([(1, 2., 'string1', 'string 4'),
                                            (2, 3., 'string2', 'string 5'),
                                            (3, 4., 'string3', 'string 6')], 
                                            schema = 'a long, b double, c string, d string')

    df_out = test_etl.spark.createDataFrame([(1, 'string1'),
                                             (2, 'string2'),
                                             (3, 'string3')], 
                                             schema = 'a long, c string')

    df_test = test_etl.t_select_columns(df_in,['a', 'c'], type='selected')
    chispa.assert_df_equality(df_out, df_test)

def test_t_select_columns_3():
    df_in = test_etl.spark.createDataFrame([(1, 2., 'string1', 'string 4'),
                                            (2, 3., 'string2', 'string 5'),
                                            (3, 4., 'string3', 'string 6')], 
                                            schema = 'a long, b double, c string, d string')

    df_out = test_etl.spark.createDataFrame([(1, 'string1'),
                                             (2, 'string2'),
                                             (3, 'string3')], 
                                             schema = 'a long, c string')

    df_test = test_etl.t_select_columns(df_in,['b', 'd'], type='other')
    chispa.assert_df_equality(df_out, df_test)

def test_t_filter_isin_source_1():
        df_in = test_etl.spark.createDataFrame([(1, 2., 'string1', 'string 4', 1),
                                            (2, 3., 'string2', 'string 5', 2),
                                            (3, 4., 'string3', 'string 6', 1),
                                            (4, 5., 'string4', 'string 7', 3),
                                            (5, 6., 'string5', 'string 8', 2)], 
                                            schema = 'a long, b double, c string, d string, e int')
        df_out = test_etl.spark.createDataFrame([(1, 2., 'string1', 'string 4', 1),
                                            (2, 3., 'string2', 'string 5', 2),
                                            (3, 4., 'string3', 'string 6', 1),
                                            (5, 6., 'string5', 'string 8', 2)], 
                                            schema = 'a long, b double, c string, d string, e int')
        df_test = test_etl.t_filter_isin_source(df_in, df_in.e, [1,2])
        chispa.assert_df_equality(df_out, df_test)

def test_t_filter_isin_source_2():
        df_in = test_etl.spark.createDataFrame([(1, 2., 'string1', 'string 4', 'Y'),
                                            (2, 3., 'string2', 'string 5', 'N'),
                                            (3, 4., 'string3', 'string 6', 'Y'),
                                            (4, 5., 'string4', 'string 7', 'X'),
                                            (5, 6., 'string5', 'string 8', 'N')], 
                                            schema = 'a long, b double, c string, d string, e string')
        df_out = test_etl.spark.createDataFrame([(1, 2., 'string1', 'string 4', 'Y'),
                                            (2, 3., 'string2', 'string 5', 'N'),
                                            (3, 4., 'string3', 'string 6', 'Y'),
                                            (5, 6., 'string5', 'string 8', 'N')], 
                                            schema = 'a long, b double, c string, d string, e string')
        df_test = test_etl.t_filter_isin_source(df_in, df_in.e, ['Y', 'N'])
        chispa.assert_df_equality(df_out, df_test)

def test_t_rename_columns():
    df_in = test_etl.spark.createDataFrame([(1, 2., 'string1', 'string 4'),
                                            (2, 3., 'string2', 'string 5'),
                                            (3, 4., 'string3', 'string 6')], 
                                            schema = 'a long, b double, c string, d string')
    
    df_out = test_etl.spark.createDataFrame([(1, 2., 'string1', 'string 4'),
                                            (2, 3., 'string2', 'string 5'),
                                            (3, 4., 'string3', 'string 6')], 
                                            schema = 'id long, b double, country string, d string')
    df_test = test_etl.t_rename_columns(df_in, {'a': 'id', 'c': 'country'})
    chispa.assert_df_equality(df_out, df_test)

def test_t_merge_sources():
    df_in1 = test_etl.spark.createDataFrame([(1, 2., 'string1', 'string 4', 'Y'),
                                            (2, 3., 'string2', 'string 5', 'N'),
                                            (3, 4., 'string3', 'string 6', 'Y'),
                                            (4, 5., 'string4', 'string 7', 'X'),
                                            (5, 6., 'string5', 'string 8', 'N')], 
                                            schema = 'a long, b double, c string, d string, e string') 

    df_in2 = test_etl.spark.createDataFrame([(1, 'test 1'),
                                            (2, 'test 2'),
                                            (3, 'test 3')], 
                                            schema = 'id long, desc string')

    df_out = test_etl.spark.createDataFrame([(1, 2., 'string1', 'string 4', 'Y', 1, 'test 1'),
                                            (2, 3., 'string2', 'string 5', 'N', 2, 'test 2'),
                                            (3, 4., 'string3', 'string 6', 'Y', 3, 'test 3')], 
                                            schema = 'a long, b double, c string, d string, e string, id long, desc string')
    
    df_test = test_etl.t_merge_sources(df_in1, df_in2, [df_in1.a == df_in2.id])
    chispa.assert_df_equality(df_out, df_test)



def test_l_export_dataframe():
    _file = './tests/data/csv_file_export.csv'

    df_out = test_etl.spark.createDataFrame([('id_1', 'test_1', 'desc_1'),
                                            ('id_2', 'test_2', 'desc_2')], 
                                                schema = 'id string, test string, desc string')

    df_out = df_out.coalesce(1)


    if os.path.exists(_file):
        shutil.rmtree(_file)

    test_etl.l_export_dataframe(df_out, _file)

    assert os.path.exists(_file)


def test_e_source_csv():
    _path = './tests/data/csv_file_export.csv'

    df_test = test_etl.e_source_csv(_path)


    df_out = test_etl.spark.createDataFrame([('id_1', 'test_1', 'desc_1'),
                                            ('id_2', 'test_2', 'desc_2')], 
                                                schema = 'id string, test string, desc string')
    chispa.assert_df_equality(df_out, df_test)

    if os.path.exists(_path):
        shutil.rmtree(_path)


def test_j_bitcoin_datamart():
    _path = './tests/data/client_data.csv'

    if os.path.exists(_path):
        shutil.rmtree(_path)

    test_etl.j_bitcoin_datamart({'customer': "./files/data_sets/csv_test_inputs/dataset_one_test.csv",
                            'transations': "./files/data_sets/csv_test_inputs/dataset_two_test.csv",
                            'country_flags' : ['United Kingdom', 'Netherlands'],
                            'location' : './tests/data'})
        
    df_out = test_etl.spark.createDataFrame([('3', 'mail_3', 'Netherlands', '3', 'string_3', 'string_19'),
                                                ('6', 'mail_6', 'Netherlands', '6', 'string_6', 'string_22'),
                                                ('8', 'mail_8', 'United Kingdom', '8', 'string_8', 'string_24'),
                                                ('10', 'mail_10', 'United Kingdom', '10', 'string_10', 'string_26')],
                                                schema='id string, email string, country string, client_identifier string, bitcoin_address string, credit_card_type string'
                                                )
        
    df_test = test_etl.e_source_csv(_path)
    chispa.assert_df_equality(df_out, df_test)

    if os.path.exists(_path):
        shutil.rmtree(_path)

def test_Bitcoin_datamart_batch1():
    _path = './client_data.csv'

    if os.path.exists(_path):
        shutil.rmtree(_path)


    batch_path = os.path.dirname(os.path.abspath(__file__))
    customer_path = './test/data/csv_test_inputs/dataset_one_test.csv'
    transation_path = './test/data/csv_test_inputs/dataset_two_test.csv'
    country_flags = "'United Kingdom' 'Netherlands'"

    os.system(f'python3 {batch_path}/Bitcoin_datamart_batch.py -c "{customer_path}" -t "{transation_path}" -f {country_flags}')

    df_out = test_etl.spark.createDataFrame([('3', 'mail_3', 'Netherlands', '3', 'string_3', 'string_19'),
                                                ('6', 'mail_6', 'Netherlands', '6', 'string_6', 'string_22'),
                                                ('8', 'mail_8', 'United Kingdom', '8', 'string_8', 'string_24'),
                                                ('10', 'mail_10', 'United Kingdom', '10', 'string_10', 'string_26')],
                                                schema='id string, email string, country string, client_identifier string, bitcoin_address string, credit_card_type string'
                                                )
        
    df_test = test_etl.e_source_csv('./client_data.csv')
    chispa.assert_df_equality(df_out, df_test)
    
    if os.path.exists('./client_data.csv'):
        shutil.rmtree('./client_data.csv')

def test_Bitcoin_datamart_batch2():
    if os.path.exists('./files/client_data.csv'):
        shutil.rmtree('./files/client_data.csv')


    batch_path = os.path.dirname(os.path.abspath(__file__))
    customer_path = './files/data_sets/csv_test_inputs/dataset_one_test.csv'
    transation_path = './files/data_sets/csv_test_inputs/dataset_two_test.csv'
    country_flags = "'United Kingdom' 'Netherlands'"
    location = './files'

    os.system(f'python3 {batch_path}/Bitcoin_datamart_batch.py -c "{customer_path}" -t "{transation_path}" -f {country_flags} -l {location}')

    df_out = test_etl.spark.createDataFrame([('3', 'mail_3', 'Netherlands', '3', 'string_3', 'string_19'),
                                                ('6', 'mail_6', 'Netherlands', '6', 'string_6', 'string_22'),
                                                ('8', 'mail_8', 'United Kingdom', '8', 'string_8', 'string_24'),
                                                ('10', 'mail_10', 'United Kingdom', '10', 'string_10', 'string_26')],
                                                schema='id string, email string, country string, client_identifier string, bitcoin_address string, credit_card_type string'
                                                )
        
    df_test = test_etl.e_source_csv('./files/client_data.csv')
    chispa.assert_df_equality(df_out, df_test)
    
    if os.path.exists('./files/client_data.csv'):
        shutil.rmtree('./files/client_data.csv')



def test_spark_close_connection():
    test_etl.c_spark_session_close(test_etl.spark)
    sp = SparkSession.getActiveSession()
    assert sp == None

if __name__ == '__main__':
    pytest.main()