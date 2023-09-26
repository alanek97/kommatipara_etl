[![CI](https://github.com/alanek97/kommatipara_etl/actions/workflows/main.yml/badge.svg)](https://github.com/alanek97/kommatipara_etl/actions/workflows/main.yml)
# kommatipara Bitcoin ETL project
## Bash script
ETL program for bitcoin datamart creation. Exctract 2 files, combine and save it. \
Stored in *bitcoin_dm_creator.py* file.
```
python3 bitcoin_dm_creator.py -c CUSTOMER -t TRANSATIONS -f COUNTRY_FLAGS [COUNTRY_FLAGS ...] [-l LOCATION] [-d | --debbug | --no-debbug]
```
options:
  `-h`, `--help`            show this help message and exit \
  `-c CUSTOMER`, `--customer CUSTOMER` path for customer file \
  `-t TRANSATIONS`, `--transations TRANSATIONS` path for transation file \
  `-f COUNTRY_FLAGS [COUNTRY_FLAGS ...]`, `--country_flags COUNTRY_FLAGS [COUNTRY_FLAGS ...]` Country names for filter \
  `-l LOCATION`, `--location LOCATION` Optional: path for output csv file (default: '.')\
  `-d`, `--debbug`, `--no-debbug` ebbug - set up more detail logging messages (default: False)

# `etl_source_code` package
## `etl_kommati_para` module
### `ETLKommatiPara(SetupKommatiPara)` class

| Function  | Transformation type | Description  | Input  |   Output  | Based on |
|---|---|---|---|---|---|
| `e_source_csv ` | Exctract  | This function reads CSV file into Spark enviroment. Options: `header` = True, `sep` = ','  | `path` str  | DataFrame   | [PySpark docs](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameReader.csv.html)
| `t_select_columns`  | Transform  |  This function selects output required columns only. If `type` = 'selected' then only provided columns will be in output (defoult). If `type` = 'other' then all other columns compare to provided ones will be in output. | `df` DataFrame, `columns` list[str], `type` str   |  DataFrame | [PySpark docs](https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.DataFrame.select.html)
| `t_filter_isin_source` | Transform  | This function is to filter data based on list of values.  | `df` DataFrame, `column` Column, `criteria` list  | DataFrame  | [PySpark docs](https://spark.apache.org/docs/3.1.3/api/python/reference/api/pyspark.sql.Column.isin.html)
| `t_merge_sources` | Transform | This function is joining 2 tables using inner method. | `df1` DataFrame, `df2` DataFrame, `condition` list[Column] | DataFrame | [PySpark docs](https://spark.apache.org/docs/3.1.2/api/python/reference/api/pyspark.sql.DataFrame.join.html)
| `t_rename_columns` | Transform | This function is renaming given columns. | `df` DataFrame, `mapping` dict | DataFrame | [PySpark docs](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.withColumnRenamed.html)
| `l_export_dataframe` | Load | This function saves DataFrame to given location. | `df` DataFrame, `path` str | csv file | [PySpark docs](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameWriter.save.html)
| `j_bitcoin_datamart` | Job | This is coimbained function which exctract 2 files, transformed (select, rename, join) and save it as csv file. | `input_param` dict | csv file | `e_source_csv`, `t_select_columns`, `t_filter_isin_source`, `t_merge_sources`,  `t_rename_columns`, `l_export_dataframe`

# `utils` package
## `etl_setup` module
### `SetupKommatiPara` class

| Function  | Transformation type | Description  | Input  |   Output  | 
|---|---|---|---|---|
| `c_spark_session_init` | Control  |  This function establishes new or get existing Spark session. | None  | SparkSession   |
| `c_spark_session_close`  | Control  |  This function terminates existing Spark session. | SparkSession |  None |
| `c_logging_setup` | Control  | This function sets up logging configuration.  | `job` str  | None |

##  `arguments_parser_bitcoin` module
### `BashArguments` class

| Function  | Transformation type | Description  | Input  |   Output |
|---|---|---|---|---|
| `arg_j_bitcoin_dm` | Bash arguments  |  This function sets up bash script arguments for j_bitcoin_datamart. | None  |  dict{`customer`: str, `transations`: str, `country_flags`: str, `location`: str, `debbug`: str} |

# <span style="color:yellow"> test cases</span>
## `test_bitcoin_dm` module

| Function  | Transformation type | Description  | Tested package |   Tested module  | Tested class | Tested function | Version | Test dependency |
|---|---|---|---|---|---|---|---|---|
| `test_spark_connetion`  | Test  | Check if spark session was establish after creation of new instance of ETLKommatiPara class   | utils |  etl_setup | SetupKommatiPara | c_spark_session_init | Defoult |
| `test_t_select_columns_1`  | Test | Select given columns in the list from DataFrame  | etl_source_code  | etl_kommati_para  | ETLKommatiPara | t_select_columns | Defoult, `type` not selected manually
| `test_t_select_columns_2`  | Test | Select given columns in the list from DataFrame  | etl_source_code  | etl_kommati_para  | ETLKommatiPara | t_select_columns | `type` = 'selected'
| `test_t_select_columns_3`  | Test | Select given columns in the list from DataFrame  | etl_source_code  | etl_kommati_para  | ETLKommatiPara | t_select_columns | `type` = 'other'
| `test_t_filter_isin_source_1`  | Test | Filter DataFrame based on values in given list  | etl_source_code  | etl_kommati_para  | ETLKommatiPara | t_filter_isin_source | filter numeric values
| `test_t_filter_isin_source_2`  | Test | Filter DataFrame based on values in given list  | etl_source_code  | etl_kommati_para  | ETLKommatiPara | t_filter_isin_source | filter character values
| `test_t_rename_columns`  | Test | Renamed columns in DataFrame based on given dictonary  | etl_source_code  | etl_kommati_para  | ETLKommatiPara | t_rename_columns | Default
| `test_t_merge_sources`  | Test | Join 2 DataFrames based on given criteria  | etl_source_code  | etl_kommati_para  | ETLKommatiPara | t_merge_sources | Default
| `test_l_export_dataframe`  | Test | Check if output file of given DataFrame is saved in proper path  | etl_source_code  | etl_kommati_para  | ETLKommatiPara | l_export_dataframe | Default
| `test_e_source_csv`  | Test | Check if file is loaded correctly from csv to Spark session  | etl_source_code  | etl_kommati_para  | ETLKommatiPara | e_source_csv | Default | test_l_export_dataframe
| `test_j_bitcoin_datamart`  | Test | Bussness logic of Bitcoin datamart is correct (ETL)  | etl_source_code  | etl_kommati_para  | ETLKommatiPara | j_bitcoin_datamart | Path for export given |
| `test_Bitcoin_datamart_bash_1`  | Test | Bussness logic of Bitcoin datamart is correct (ETL) executed by Bash script  |   | bitcoin_dm_creator  |  |  | Defoult `location` (root path) and defoult `debbug` = False |
| `test_Bitcoin_datamart_bash_2`  | Test | Bussness logic of Bitcoin datamart is correct (ETL) executed by Bash script  |   | bitcoin_dm_creator  |  |  | Given `location` of final file and `debbug` = True |
| `test_spark_close_connection`  | Test  | Check if spark session was establish after creation of new instance of ETLKommatiPara class   | utils |  etl_setup | SetupKommatiPara | c_spark_session_close | Defoult |

# <span style="color:yellow"> Example log file</span>

| asctime | process | levelname | name | module | funcName | message |
|---|---|---|---|---|---|---|
26-09-23 10:54:31 | 29757 | INFO | root | etl_setup  | c_spark_session_init | Spark session is established
26-09-23 10:54:52 | 29757 | INFO | root | etl_kommati_para  | e_source_csv | file was read from: ./tests/data/dataset_one_test.csv
26-09-23 10:54:52 | 29757 | INFO | root | etl_kommati_para  | e_source_csv | Row count on this step: 10
26-09-23 10:54:53 | 29757 | INFO | root | etl_kommati_para  | e_source_csv | Column count on this step: 5
26-09-23 10:54:53 | 29757 | INFO | root | etl_kommati_para  | e_source_csv | Data Frame schema: StructType([StructField('id', StringType(), True), StructField('first_name', StringType(), True), StructField('last_name', StringType(), True), StructField('email', StringType(), True), StructField('country', StringType(), True)])
26-09-23 10:54:53 | 29757 | INFO | root | etl_kommati_para  | t_filter_isin_source | Selected values: ['United Kingdom', 'Netherlands'] for column: Column<'country'>
26-09-23 10:54:53 | 29757 | INFO | root | etl_kommati_para  | t_filter_isin_source | Row count on this step: 4
26-09-23 10:54:53 | 29757 | INFO | root | etl_kommati_para  | t_filter_isin_source | Column count on this step: 5
26-09-23 10:54:53 | 29757 | INFO | root | etl_kommati_para  | t_filter_isin_source | Data Frame schema: StructType([StructField('id', StringType(), True), StructField('first_name', StringType(), True), StructField('last_name', StringType(), True), StructField('email', StringType(), True), StructField('country', StringType(), True)])
26-09-23 10:54:53 | 29757 | INFO | root | etl_kommati_para  | t_select_columns | Selected columns: ['id', 'email', 'country'] as selected type
26-09-23 10:54:53 | 29757 | INFO | root | etl_kommati_para  | t_select_columns | Row count on this step: 4
26-09-23 10:54:53 | 29757 | INFO | root | etl_kommati_para  | t_select_columns | Column count on this step: 3
26-09-23 10:54:53 | 29757 | INFO | root | etl_kommati_para  | t_select_columns | Data Frame schema: StructType([StructField('id', StringType(), True), StructField('email', StringType(), True), StructField('country', StringType(), True)])
26-09-23 10:54:53 | 29757 | INFO | root | etl_kommati_para  | e_source_csv | file was read from: ./tests/data/dataset_two_test.csv
26-09-23 10:54:53 | 29757 | INFO | root | etl_kommati_para  | e_source_csv | Row count on this step: 16
26-09-23 10:54:53 | 29757 | INFO | root | etl_kommati_para  | e_source_csv | Column count on this step: 4
26-09-23 10:54:53 | 29757 | INFO | root | etl_kommati_para  | e_source_csv | Data Frame schema: StructType([StructField('id', StringType(), True), StructField('btc_a', StringType(), True), StructField('cc_t', StringType(), True), StructField('cc_n', StringType(), True)])
26-09-23 10:54:53 | 29757 | INFO | root | etl_kommati_para  | t_select_columns | Selected columns: ['cc_n'] as other type
26-09-23 10:54:53 | 29757 | INFO | root | etl_kommati_para  | t_select_columns | Row count on this step: 16
26-09-23 10:54:53 | 29757 | INFO | root | etl_kommati_para  | t_select_columns | Column count on this step: 3
26-09-23 10:54:53 | 29757 | INFO | root | etl_kommati_para  | t_select_columns | Data Frame schema: StructType([StructField('id', StringType(), True), StructField('btc_a', StringType(), True), StructField('cc_t', StringType(), True)])
26-09-23 10:54:53 | 29757 | INFO | root | etl_kommati_para  | t_rename_columns | Column renamed: {'id': 'client_identifier', 'btc_a': 'bitcoin_address', 'cc_t': 'credit_card_type'}
26-09-23 10:54:53 | 29757 | INFO | root | etl_kommati_para  | t_rename_columns | Row count on this step: 16
26-09-23 10:54:53 | 29757 | INFO | root | etl_kommati_para  | t_rename_columns | Column count on this step: 3
26-09-23 10:54:53 | 29757 | INFO | root | etl_kommati_para  | t_rename_columns | Data Frame schema: StructType([StructField('client_identifier', StringType(), True), StructField('bitcoin_address', StringType(), True), StructField('credit_card_type', StringType(), True)])
26-09-23 10:54:53 | 29757 | INFO | root | etl_kommati_para  | t_merge_sources | Join step. Join on [Column<'(id = client_identifier)'>]
26-09-23 10:54:54 | 29757 | INFO | root | etl_kommati_para  | t_merge_sources | Row count on this step: 4
26-09-23 10:54:54 | 29757 | INFO | root | etl_kommati_para  | t_merge_sources | Column count on this step: 6
26-09-23 10:54:54 | 29757 | INFO | root | etl_kommati_para  | t_merge_sources | Data Frame schema: StructType([StructField('id', StringType(), True), StructField('email', StringType(), True), StructField('country', StringType(), True), StructField('client_identifier', StringType(), True), StructField('bitcoin_address', StringType(), True), StructField('credit_card_type', StringType(), True)])
26-09-23 10:54:54 | 29757 | INFO | root | etl_kommati_para  | l_export_dataframe | Dataframe saved in: ./tests/data/client_data.csv
26-09-23 10:54:54 | 29757 | INFO | root | etl_kommati_para  | l_export_dataframe | Row count on this step: 4
26-09-23 10:54:54 | 29757 | INFO | root | etl_kommati_para  | l_export_dataframe | Column count on this step: 6
26-09-23 10:54:54 | 29757 | INFO | root | etl_kommati_para  | l_export_dataframe | Data Frame schema: StructType([StructField('id', StringType(), True), StructField('email', StringType(), True), StructField('country', StringType(), True), StructField('client_identifier', StringType(), True), StructField('bitcoin_address', StringType(), True), StructField('credit_card_type', StringType(), True)])
26-09-23 10:54:54 | 29757 | INFO | py4j.clientserver | clientserver  | close | Closing down clientserver connection