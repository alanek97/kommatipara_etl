
from  ETL_kommatiPara import ETLKommatiPara
from pyspark.sql import SparkSession
import pytest
import chispa


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


def test_spark_close_connection():
    test_etl.c_spark_session_close(test_etl.spark)
    sp = SparkSession.getActiveSession()
    assert sp == None

if __name__ == '__main__':
    pytest.main()