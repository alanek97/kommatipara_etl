[![CI](https://github.com/alanek97/kommatipara_etl/actions/workflows/main.yml/badge.svg)](https://github.com/alanek97/kommatipara_etl/actions/workflows/main.yml)

# kommatipara Bitcoin ETL project

The main package for project is `source`. It contains 4 modules:

-   `bitcoin_dm_creator`
    -   class: `__main__` - executor for bash script
-   `etl_kommati_para` 
    -   class: `ETLKommatiPara(SetupKommatiPara)` - ETL source code
-   `etl_setup`
    -   class: `SetupKommatiPara`- setting for ETL, like spark connection, logging ect.
-   `arguments_parser_bitcoin` 

    -   class: `BashArguments` - declaration of bash arguments

    Each function inside class is described below.

## Github actions

Github actions are set up to run on each push [main and development branches] and pull-request [main branch]. \
Each run of Github action job produces 2 artifact:

1.  pytest unit test results - testresults of pytest and logging file.
2.  ETL kommatipara wheel file - python wheel file of package 

## Bash script

ETL program for bitcoin datamart creation. Exctract 2 files, combine and save it. \
Stored in *bitcoin_dm_creator.py* file.

    python3 bitcoin_dm_creator.py -c CUSTOMER -t TRANSATIONS -f COUNTRY_FLAGS [COUNTRY_FLAGS ...] [-l LOCATION] [-d | --debbug | --no-debbug]

options:
  `-h`, `--help`            show this help message and exit \
  `-c CUSTOMER`, `--customer CUSTOMER` path for customer file \
  `-t TRANSATIONS`, `--transations TRANSATIONS` path for transation file \
  `-f COUNTRY_FLAGS [COUNTRY_FLAGS ...]`, `--country_flags COUNTRY_FLAGS [COUNTRY_FLAGS ...]` Country names for filter \
  `-l LOCATION`, `--location LOCATION` Optional: path for output csv file (default: '.')\
  `-d`, `--debbug`, `--no-debbug` ebbug - set up more detail logging messages (default: False)

# `source` package

## `etl_kommati_para` module

### `ETLKommatiPara(SetupKommatiPara)` class

| Function               | Transformation type | Description                                                                                                                                                                                                               | Input                                                      | Output    | Based on                                                                                                                               |
| ---------------------- | ------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------- | --------- | -------------------------------------------------------------------------------------------------------------------------------------- |
| `e_source_csv`         | Exctract            | This function reads CSV file into Spark enviroment. Options: `header` = True, `sep` = ','                                                                                                                                 | `path` str                                                 | DataFrame | [PySpark docs](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameReader.csv.html)         |
| `t_select_columns`     | Transform           | This function selects output required columns only. If `type` = 'selected' then only provided columns will be in output (defoult). If `type` = 'other' then all other columns compare to provided ones will be in output. | `df` DataFrame, `columns` list[str], `type` str            | DataFrame | [PySpark docs](https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.DataFrame.select.html)                         |
| `t_filter_isin_source` | Transform           | This function is to filter data based on list of values.                                                                                                                                                                  | `df` DataFrame, `column` Column, `criteria` list           | DataFrame | [PySpark docs](https://spark.apache.org/docs/3.1.3/api/python/reference/api/pyspark.sql.Column.isin.html)                              |
| `t_merge_sources`      | Transform           | This function is joining 2 tables using inner method.                                                                                                                                                                     | `df1` DataFrame, `df2` DataFrame, `condition` list[Column] | DataFrame | [PySpark docs](https://spark.apache.org/docs/3.1.2/api/python/reference/api/pyspark.sql.DataFrame.join.html)                           |
| `t_rename_columns`     | Transform           | This function is renaming given columns.                                                                                                                                                                                  | `df` DataFrame, `mapping` dict                             | DataFrame | [PySpark docs](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.withColumnRenamed.html) |
| `l_export_dataframe`   | Load                | This function saves DataFrame to given location.                                                                                                                                                                          | `df` DataFrame, `path` str                                 | csv file  | [PySpark docs](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameWriter.save.html)        |
| `j_bitcoin_datamart`   | Job                 | This is coimbained function which exctract 2 files, transformed (select, rename, join) and save it as csv file.                                                                                                           | `input_param` dict                                         | csv file  | `e_source_csv`, `t_select_columns`, `t_filter_isin_source`, `t_merge_sources`,  `t_rename_columns`, `l_export_dataframe`               |

## `etl_setup` module

### `SetupKommatiPara` class

| Function                | Transformation type | Description                                                  | Input        | Output       |
| ----------------------- | ------------------- | ------------------------------------------------------------ | ------------ | ------------ |
| `c_spark_session_init`  | Control             | This function establishes new or get existing Spark session. | None         | SparkSession |
| `c_spark_session_close` | Control             | This function terminates existing Spark session.             | SparkSession | None         |
| `c_logging_setup`       | Control             | This function sets up logging configuration.                 | `job` str    | None         |

## `arguments_parser_bitcoin` module

### `BashArguments` class

| Function           | Transformation type | Description                                                         | Input | Output                                                                                          |
| ------------------ | ------------------- | ------------------------------------------------------------------- | ----- | ----------------------------------------------------------------------------------------------- |
| `arg_j_bitcoin_dm` | Bash arguments      | This function sets up bash script arguments for j_bitcoin_datamart. | None  | dict{`customer`: str, `transations`: str, `country_flags`: str, `location`: str, `debbug`: str} |

#  test cases

## `test_bitcoin_dm` module

| Function                       | Transformation type | Description                                                                                 | Tested package | Tested module      | Tested class     | Tested function       | Version                                                     | Test dependency         |
| ------------------------------ | ------------------- | ------------------------------------------------------------------------------------------- | -------------- | ------------------ | ---------------- | --------------------- | ----------------------------------------------------------- | ----------------------- |
| `test_spark_connetion`         | Test                | Check if spark session was establish after creation of new instance of ETLKommatiPara class | source         | etl_setup          | SetupKommatiPara | c_spark_session_init  | Defoult                                                     |                         |
| `test_t_select_columns_1`      | Test                | Select given columns in the list from DataFrame                                             | source         | etl_kommati_para   | ETLKommatiPara   | t_select_columns      | Defoult, `type` not selected manually                       |                         |
| `test_t_select_columns_2`      | Test                | Select given columns in the list from DataFrame                                             | source         | etl_kommati_para   | ETLKommatiPara   | t_select_columns      | `type` = 'selected'                                         |                         |
| `test_t_select_columns_3`      | Test                | Select given columns in the list from DataFrame                                             | source         | etl_kommati_para   | ETLKommatiPara   | t_select_columns      | `type` = 'other'                                            |                         |
| `test_t_filter_isin_source_1`  | Test                | Filter DataFrame based on values in given list                                              | source         | etl_kommati_para   | ETLKommatiPara   | t_filter_isin_source  | filter numeric values                                       |                         |
| `test_t_filter_isin_source_2`  | Test                | Filter DataFrame based on values in given list                                              | source         | etl_kommati_para   | ETLKommatiPara   | t_filter_isin_source  | filter character values                                     |                         |
| `test_t_rename_columns`        | Test                | Renamed columns in DataFrame based on given dictonary                                       | source         | etl_kommati_para   | ETLKommatiPara   | t_rename_columns      | Default                                                     |                         |
| `test_t_merge_sources`         | Test                | Join 2 DataFrames based on given criteria                                                   | source         | etl_kommati_para   | ETLKommatiPara   | t_merge_sources       | Default                                                     |                         |
| `test_l_export_dataframe`      | Test                | Check if output file of given DataFrame is saved in proper path                             | source         | etl_kommati_para   | ETLKommatiPara   | l_export_dataframe    | Default                                                     |                         |
| `test_e_source_csv`            | Test                | Check if file is loaded correctly from csv to Spark session                                 | source         | etl_kommati_para   | ETLKommatiPara   | e_source_csv          | Default                                                     | test_l_export_dataframe |
| `test_j_bitcoin_datamart`      | Test                | Bussness logic of Bitcoin datamart is correct (ETL)                                         | source         | etl_kommati_para   | ETLKommatiPara   | j_bitcoin_datamart    | Path for export given                                       |                         |
| `test_Bitcoin_datamart_bash_1` | Test                | Bussness logic of Bitcoin datamart is correct (ETL) executed by Bash script                 | source         | bitcoin_dm_creator |                  |                       | Defoult `location` (root path) and defoult `debbug` = False |                         |
| `test_Bitcoin_datamart_bash_2` | Test                | Bussness logic of Bitcoin datamart is correct (ETL) executed by Bash script                 | source         | bitcoin_dm_creator |                  |                       | Given `location` of final file and `debbug` = True          |                         |
| `test_spark_close_connection`  | Test                | Check if spark session was establish after creation of new instance of ETLKommatiPara class | source         | etl_setup          | SetupKommatiPara | c_spark_session_close | Defoult                                                     |                         |
