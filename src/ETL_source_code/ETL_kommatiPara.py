
from pyspark.sql import DataFrame, Column
import os
from setUp_kommatiPara import setUpKommatiPara


class ETLKommatiPara(setUpKommatiPara):

    def __init__(self):
        self.spark = self.c_spark_session_init()
        self.debbug = False

    def e_source_csv(self, path: str) -> DataFrame:
        '''
        Type of method: exctract

        This function reads CSV file into Spark enviroment.
        Options: header = True, sep = ','

        input: [path] str
        output: DataFrame

        '''
        df = self.spark.read.options(header = True, sep = ',').csv(path)
        return df

    def t_select_columns(self, df: DataFrame, columns: list, type: str  = 'selected') -> DataFrame:
        '''
        Type of method: transform

        This function selects output required columns only.
        If [type] = 'selected' then only provided columns will be in output (defoult).
        If [type] = 'other' then all other columns compare to provided ones will be in output.

        input: [df] DataFrame,
                [columns] list[str],
                [type] str
        output: DataFrame
        '''
        if type == 'selected':
            df = df.select(*columns)
        if type == 'other':
            df = df.drop(*columns)
        return df

    def t_filter_isin_source(self, df: DataFrame, column: Column, criteria: list) -> DataFrame:
        '''
        Type of method: tranform
        '''
        df = df[column.isin(criteria)]
        return df

    def t_merge_sources(self, df1: DataFrame, df2: DataFrame, condition: list) -> DataFrame:
        '''
        Type of method: transform
        '''
        df = df1.join(df2, on = condition, how = 'inner')
        return df

    def t_rename_columns(self, df: DataFrame, mapping: dict) -> DataFrame:
        '''
        Type of method: transform
        '''
        for old_column, new_column in mapping.items():
            df = df.withColumnRenamed(old_column, new_column)
        return df

    def l_export_dataframe(self, df: DataFrame, path: str) -> None:
        '''
        Type of method: load
        '''
        df.write.mode("overwrite").options(header=True).format('CSV').save(path)

    def j_bitcoin_datamart(self, input_param: dict) -> None:
        '''
        Type of method: Job
        '''
        df_customers = self.e_source_csv(input_param['customer'])
        df_cust_filter = self.t_filter_isin_source(df_customers,df_customers.country,input_param['country_flags'])
        df_cust_flr_select = self.t_select_columns(df_cust_filter,['id', 'email', 'country'])
        df_transations = self.e_source_csv(input_param['transations'])
        df_trans_select = self.t_select_columns(df_transations,['cc_n'], type = 'other')
        df_trans_renamed = self.t_rename_columns(df_trans_select, {'id':'client_identifier', 'btc_a': 'bitcoin_address', 'cc_t': 'credit_card_type'})
        df_merged = self.t_merge_sources(df_cust_flr_select, df_trans_renamed,  [df_cust_flr_select.id == df_trans_renamed.client_identifier])
        self.l_export_dataframe(df_merged,'/home/alan/kommatiPara/source_files/data_sets/client_data.csv')