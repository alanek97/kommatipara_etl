from pyspark.errors import PySparkException
from pyspark.sql import DataFrame, Column
import logging
from source.etl_setup import SetupKommatiPara


class ExtractKommatiPara():
    def extract_source_csv(self, path: str) -> DataFrame:
        """
        This function reads CSV file into Spark environment.
        Options: header = True, sep = ','

        input: [path] str
        output: DataFrame

        """
        try:
            df = self.spark.read.options(
                header=True, sep=',', inferSchema=True).csv(path)

            logging.info(f'file was read from: {path}')
            logging.debug(f'Row count on this step: {df.count()}')
            logging.debug(f'Column count on this step: {len(df.columns)}')
            logging.debug(f'Data Frame schema: {str(df.schema)}')
            return df
        except PySparkException as err:
            logging.error('Pyspark error')
            logging.error(err)
            raise
        except Exception as e:
            logging.error(e)
            raise


class TransformKommatiPara():
    def transform_select_columns(self, df: DataFrame, columns: list, type: str = 'selected') -> DataFrame:
        """""
        This function selects output required columns only.
        If [type] = 'selected' then only provided columns will be in output (default).
        If [type] = 'other' then all other columns compare to provided ones will be in output.

        input: [df] DataFrame,
                [columns] list[str],
                [type] str
        output: DataFrame
        """
        if type == 'selected':
            df = df.select(*columns)
        if type == 'other':
            df = df.drop(*columns)

        logging.info(f'Selected columns: {columns} as {type} type')
        logging.debug(f'Row count on this step: {df.count()}')
        logging.debug(f'Column count on this step: {len(df.columns)}')
        logging.debug(f'Data Frame schema: {str(df.schema)}')
        return df

    def transform_filter_isin_source(self, df: DataFrame, column: Column, criteria: list) -> DataFrame:
        """
        This function is to filter data based on list of values.

        input: [df] DataFrame,
                [column] Column,
                [criteria] list
        output: DataFrame
        """
        df = df[column.isin(criteria)]

        logging.info(f'Selected values: {criteria} for column: {column}')
        logging.debug(f'Row count on this step: {df.count()}')
        logging.debug(f'Column count on this step: {len(df.columns)}')
        logging.debug(f'Data Frame schema: {str(df.schema)}')
        return df

    def transform_join_sources(self, df1: DataFrame, df2: DataFrame, condition: list) -> DataFrame:
        """
        This function is joining 2 tables using inner method.

        input: [df1] DataFrame,
                [df2] DataFrame,
                [condition] list[Column]
        output: DataFrame
        """
        df = df1.join(df2, on=condition, how='inner')

        logging.info(f'Join step. Join on {condition}')
        logging.debug(f'Row count on this step: {df.count()}')
        logging.debug(f'Column count on this step: {len(df.columns)}')
        logging.debug(f'Data Frame schema: {str(df.schema)}')
        return df

    def transform_rename_columns(self, df: DataFrame, mapping: dict) -> DataFrame:
        """
        This function is renaming given columns.

        input: [df] DataFrame
                [mapping] dict
        output: DataFrame
        """
        for old_column, new_column in mapping.items():
            df = df.withColumnRenamed(old_column, new_column)

        logging.info(f'Column renamed: {str(mapping)}')
        logging.debug(f'Row count on this step: {df.count()}')
        logging.debug(f'Column count on this step: {len(df.columns)}')
        logging.debug(f'Data Frame schema: {str(df.schema)}')
        return df


class LoadKommatiPara():
    def load_export_dataframe(self, df: DataFrame, path: str) -> None:
        """
        This function saves DataFrame to given location.

        input: [df] DataFrame,
                [path] str
        output: csv file 
        """
        try:
            df.write.mode("overwrite").options(
                header=True).format('CSV').save(path)

            logging.info(f'DataFrame saved in: {path}')
            logging.debug(f'Row count on this step: {df.count()}')
            logging.debug(f'Column count on this step: {len(df.columns)}')
            logging.debug(f'Data Frame schema: {str(df.schema)}')
        except PySparkException as err:
            logging.error('Pyspark error')
            logging.error(err)
            raise
        except Exception as e:
            logging.error(e)
            raise


class JobKommatiPara(SetupKommatiPara, ExtractKommatiPara, TransformKommatiPara, LoadKommatiPara):
    def __init__(self, log_level='info'):
        self.control_logging_setup('Bitcoin_dm_log', log_level)
        self.spark = self.control_spark_session_init()

    def job_bitcoin_datamart(self, input_param: dict) -> None:
        """
        This is combined function which extract 2 files, transformed (select, rename, join) and save it as csv file.

        input: [input_param] dict
        output: csv file
        """
        df_customers = self.extract_source_csv(input_param['customer'])
        df_cust_filter = self.transform_filter_isin_source(
            df_customers, df_customers.country, input_param['country_flags'])
        df_cust_flr_select = self.transform_select_columns(
            df_cust_filter, ['id', 'email', 'country'])
        df_transactions = self.extract_source_csv(input_param['transactions'])
        df_trans_select = self.transform_select_columns(
            df_transactions, ['cc_n'], type='other')
        df_trans_renamed = self.transform_rename_columns(df_trans_select, {
            'id': 'client_identifier', 'btc_a': 'bitcoin_address', 'cc_t': 'credit_card_type'})
        df_merged = self.transform_join_sources(df_cust_flr_select, df_trans_renamed,  [
            df_cust_flr_select.id == df_trans_renamed.client_identifier])
        self.load_export_dataframe(
            df_merged, input_param['location'] + '/client_data.csv')
