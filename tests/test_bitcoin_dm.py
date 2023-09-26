
from etl_source_code.etl_kommati_para import ETLKommatiPara
from pyspark.sql import SparkSession
import pytest
import chispa
import os
import shutil

test_etl = ETLKommatiPara()

def test_spark_connetion():
    '''
    Tested package: utils
    Tested module: etl_setup
    Tested class: SetupKommatiPara
    Tested function: c_spark_session_init
    Test case: Check if spark session was establish after creation of new instance of ETLKommatiPara class 
    Version: Defoult
    '''
    sp = SparkSession.getActiveSession()
    assert sp != None

def test_t_select_columns_1():
    '''
    Tested package: etl_source_code
    Tested module: etl_kommati_para
    Tested class: ETLKommatiPara
    Tested function: t_select_columns
    Test case: Select given columns in the list from DataFrame
    Version: Defoult, type not selected manually
    '''
    df_in = test_etl.spark.createDataFrame([(1, 2., 'string1', 'string 4'),
                                            (2, 3., 'string2', 'string 5'),
                                            (3, 4., 'string3', 'string 6')], 
                                            schema = 'a long, b double, c string, d string')

    df_out = test_etl.spark.createDataFrame([(1, 'string1'),
                                             (2, 'string2'),
                                             (3, 'string3')], 
                                             schema = 'a long, c string')

    df_test = test_etl.t_select_columns(df_in,['a', 'c'])
    chispa.assert_df_equality(df_out, df_test, ignore_row_order=True)

def test_t_select_columns_2():
    '''
    Tested package: etl_source_code
    Tested module: etl_kommati_para
    Tested class: ETLKommatiPara
    Tested function: t_select_columns
    Test case: Select given columns in the list from DataFrame
    Version: type = 'selected'
    '''
    df_in = test_etl.spark.createDataFrame([(1, 2., 'string1', 'string 4'),
                                            (2, 3., 'string2', 'string 5'),
                                            (3, 4., 'string3', 'string 6')], 
                                            schema = 'a long, b double, c string, d string')

    df_out = test_etl.spark.createDataFrame([(1, 'string1'),
                                             (2, 'string2'),
                                             (3, 'string3')], 
                                             schema = 'a long, c string')

    df_test = test_etl.t_select_columns(df_in,['a', 'c'], type='selected')
    chispa.assert_df_equality(df_out, df_test, ignore_row_order=True)

def test_t_select_columns_3():
    '''
    Tested package: etl_source_code
    Tested module: etl_kommati_para
    Tested class: ETLKommatiPara
    Tested function: t_select_columns
    Test case: Select other columns from dataFrame than in the list
    Version: type = 'other'
    '''
    df_in = test_etl.spark.createDataFrame([(1, 2., 'string1', 'string 4'),
                                            (2, 3., 'string2', 'string 5'),
                                            (3, 4., 'string3', 'string 6')], 
                                            schema = 'a long, b double, c string, d string')

    df_out = test_etl.spark.createDataFrame([(1, 'string1'),
                                             (2, 'string2'),
                                             (3, 'string3')], 
                                             schema = 'a long, c string')

    df_test = test_etl.t_select_columns(df_in,['b', 'd'], type='other')
    chispa.assert_df_equality(df_out, df_test, ignore_row_order=True)

def test_t_filter_isin_source_1():
    '''
    Tested package: etl_source_code
    Tested module: etl_kommati_para
    Tested class: ETLKommatiPara
    Tested function: t_filter_isin_source
    Test case: Filter DataFrame based on values in given list
    Version: Filter numeric values
    '''
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
    chispa.assert_df_equality(df_out, df_test, ignore_row_order=True)

def test_t_filter_isin_source_2():
    '''
    Tested package: etl_source_code
    Tested module: etl_kommati_para
    Tested class: ETLKommatiPara
    Tested function: t_filter_isin_source
    Test case: Filter DataFrame based on values in given list
    Version: Filter character values
    '''
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
    chispa.assert_df_equality(df_out, df_test, ignore_row_order=True)

def test_t_rename_columns():
    '''
    Tested package: etl_source_code
    Tested module: etl_kommati_para
    Tested class: ETLKommatiPara
    Tested function: t_rename_columns
    Test case: Renamed columns in DataFrame based on given dictonary
    Version: Default
    '''
    df_in = test_etl.spark.createDataFrame([(1, 2., 'string1', 'string 4'),
                                            (2, 3., 'string2', 'string 5'),
                                            (3, 4., 'string3', 'string 6')], 
                                            schema = 'a long, b double, c string, d string')
    
    df_out = test_etl.spark.createDataFrame([(1, 2., 'string1', 'string 4'),
                                            (2, 3., 'string2', 'string 5'),
                                            (3, 4., 'string3', 'string 6')], 
                                            schema = 'id long, b double, country string, d string')
    df_test = test_etl.t_rename_columns(df_in, {'a': 'id', 'c': 'country'})
    chispa.assert_df_equality(df_out, df_test, ignore_row_order=True)

def test_t_merge_sources():
    '''
    Tested package: etl_source_code
    Tested module: etl_kommati_para
    Tested class: ETLKommatiPara
    Tested function: t_merge_sources
    Test case: Join 2 DataFrames based on given criteria
    Version: Default
    '''
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
    chispa.assert_df_equality(df_out, df_test, ignore_row_order=True)

def test_l_export_dataframe():
    '''
    Tested package: etl_source_code
    Tested module: etl_kommati_para
    Tested class: ETLKommatiPara
    Tested function: l_export_dataframe
    Test case: Check if output file of given DataFrame is saved in proper path
    Version: Default
    '''
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
    '''
    Tested package: etl_source_code
    Tested module: etl_kommati_para
    Tested class: ETLKommatiPara
    Tested function: e_source_csv
    Test case: Check if file is loaded correctly from csv to Spark session
    Test dependency: test_l_export_dataframe
    Version: Default
    '''
    _path = './tests/data/csv_file_export.csv'

    df_test = test_etl.e_source_csv(_path)

    df_out = test_etl.spark.createDataFrame([('id_1', 'test_1', 'desc_1'),
                                            ('id_2', 'test_2', 'desc_2')], 
                                                schema = 'id string, test string, desc string')
    chispa.assert_df_equality(df_out, df_test, ignore_row_order=True)

    if os.path.exists(_path):
        shutil.rmtree(_path)


def test_j_bitcoin_datamart():
    '''
    Tested package: etl_source_code
    Tested module: etl_kommati_para
    Tested class: ETLKommatiPara
    Tested function: j_bitcoin_datamart
    Test case: Bussness logic of Bitcoin datamart is correct (ETL)
    Version: Path for export given
    '''
    _path = './tests/data/client_data.csv'

    if os.path.exists(_path):
        shutil.rmtree(_path)

    test_etl.j_bitcoin_datamart({'customer': "./tests/data/dataset_one_test.csv",
                            'transations': "./tests/data/dataset_two_test.csv",
                            'country_flags' : ['United Kingdom', 'Netherlands'],
                            'location' : os.path.dirname(_path)})
        
    df_out = test_etl.spark.createDataFrame([('3', 'mail_3', 'Netherlands', '3', 'string_3', 'string_19'),
                                                ('6', 'mail_6', 'Netherlands', '6', 'string_6', 'string_22'),
                                                ('8', 'mail_8', 'United Kingdom', '8', 'string_8', 'string_24'),
                                                ('10', 'mail_10', 'United Kingdom', '10', 'string_10', 'string_26')],
                                                schema='id string, email string, country string, client_identifier string, bitcoin_address string, credit_card_type string'
                                                )
        
    df_test = test_etl.e_source_csv(_path)
    chispa.assert_df_equality(df_out, df_test, ignore_row_order=True)

    if os.path.exists(_path):
        shutil.rmtree(_path)

def test_Bitcoin_datamart_batch_1():
    '''
    Tested module: bitcoin_dm_creator
    Test case: Bussness logic of Bitcoin datamart is correct (ETL) executed by Bash script
    Version: Defoult path (root path) and defoult debbug = False
    '''
    _path = './client_data.csv'

    if os.path.exists(_path):
        shutil.rmtree(_path)

    batch_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    customer_path = './tests/data/dataset_one_test.csv'
    transation_path = './tests/data/dataset_two_test.csv'
    country_flags = "'United Kingdom' 'Netherlands'"

    os.system(f'python3 {batch_path}/src/bitcoin_dm_creator.py -c "{customer_path}" -t "{transation_path}" -f {country_flags}')

    df_out = test_etl.spark.createDataFrame([('3', 'mail_3', 'Netherlands', '3', 'string_3', 'string_19'),
                                                ('6', 'mail_6', 'Netherlands', '6', 'string_6', 'string_22'),
                                                ('8', 'mail_8', 'United Kingdom', '8', 'string_8', 'string_24'),
                                                ('10', 'mail_10', 'United Kingdom', '10', 'string_10', 'string_26')],
                                                schema='id string, email string, country string, client_identifier string, bitcoin_address string, credit_card_type string'
                                                )
        
    df_test = test_etl.e_source_csv(_path)
    chispa.assert_df_equality(df_out, df_test, ignore_row_order=True)
    
    if os.path.exists(_path):
        shutil.rmtree(_path)

def test_Bitcoin_datamart_batch_2():
    '''
    Tested module: bitcoin_dm_creator
    Test case: Bussness logic of Bitcoin datamart is correct (ETL) executed by Bash script.
    Version: Given location of final file and debbug = True
    '''
    _path = './tests/data/client_data.csv'

    if os.path.exists(_path):
        shutil.rmtree(_path)

    batch_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    customer_path = './tests/data/dataset_one_test.csv'
    transation_path = './tests/data/dataset_two_test.csv'
    country_flags = "'United Kingdom' 'Netherlands'"
    location = os.path.dirname(_path)

    os.system(f'python3 {batch_path}/src/bitcoin_dm_creator.py -c "{customer_path}" -t "{transation_path}" -f {country_flags} -l {location} -d --debbug')

    df_out = test_etl.spark.createDataFrame([('3', 'mail_3', 'Netherlands', '3', 'string_3', 'string_19'),
                                                ('6', 'mail_6', 'Netherlands', '6', 'string_6', 'string_22'),
                                                ('8', 'mail_8', 'United Kingdom', '8', 'string_8', 'string_24'),
                                                ('10', 'mail_10', 'United Kingdom', '10', 'string_10', 'string_26')],
                                                schema='id string, email string, country string, client_identifier string, bitcoin_address string, credit_card_type string'
                                                )
        
    df_test = test_etl.e_source_csv(_path)
    chispa.assert_df_equality(df_out, df_test, ignore_row_order=True)
    
    if os.path.exists(_path):
        shutil.rmtree(_path)

def test_spark_close_connection():
    '''
    Tested package: utils
    Tested module: etl_setup
    Tested class: SetupKommatiPara
    Tested function: c_spark_session_init
    Test case: Check if spark session was terminated 
    Version: Defoult
    '''
    test_etl.c_spark_session_close(test_etl.spark)
    sp = SparkSession.getActiveSession()
    assert sp == None

if __name__ == '__main__':
    pytest.main()