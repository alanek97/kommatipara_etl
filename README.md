[![CI](https://github.com/alanek97/kommatipara_etl/actions/workflows/main.yml/badge.svg)](https://github.com/alanek97/kommatipara_etl/actions/workflows/main.yml)

# kommatipara Bitcoin ETL project

The main package for project is `source`. It contains 4 modules:

-   `__main__`
    -   class: `__main__` - executor for CLI script
-   `etl_kommati_para` 
    -   class: `JobKommatiPara(SetupKommatiPara, ExtractKommatiPara, TransformKommatiPara, LoadKommatiPara)` - ETL source code
-   `etl_setup`
    -   class: `SetupKommatiPara`- setting for ETL, like spark connection, logging ect.
-   `arguments_parser` 

    -   class: `ParserArguments` - declaration of bash arguments

    Each function inside class is described below.

## Github actions

Github actions are set up to run on each push [main and development branches] and pull-request [main branch]. \
Each run of Github action job produces 2 artifact:

1.  pytest unit and integration tests results - test results of pytest and logging file.
2.  ETL kommatipara wheel file - python wheel file of package 

## CLI script

ETL program for bitcoin datamart creation. Extract 2 files, combine and save it. \
Stored in *__main__.py* file.

    python3 __main__.py -c CUSTOMER -t TRANSACTIONS -f COUNTRY_FLAGS [COUNTRY_FLAGS ...] [-l LOCATION] [-d | --debug | --no-debug]

options:
  `-h`, `--help`            show this help message and exit \
  `-c CUSTOMER`, `--customer CUSTOMER` path for customer file \
  `-t TRANSACTIONS`, `--transactions TRANSACTIONS` path for transaction file \
  `-f COUNTRY_FLAGS [COUNTRY_FLAGS ...]`, `--country_flags COUNTRY_FLAGS [COUNTRY_FLAGS ...]` Country names for filter \
  `-l LOCATION`, `--location LOCATION` Optional: path for output csv file (default: '.')\
  `-d`, `--debug`, `--no-debug` debug - set up more detail logging messages (default: False)

# `source` package

## `etl_kommati_para` module

### `JobKommatiPara(SetupKommatiPara, ExtractKommatiPara, TransformKommatiPara, LoadKommatiPara)` class

| Function               | Transformation type | Description                                                                                                                                                                                                               | Input                                                      | Output    | Based on                                                                                                                               |
| ---------------------- | ------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------- | --------- | -------------------------------------------------------------------------------------------------------------------------------------- |
| `extract_source_csv`         | Exctract            | This function reads CSV file into Spark environment. Options: `header` = True, `sep` = ','                                                                                                                                 | `path` str                                                 | DataFrame | [PySpark docs](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameReader.csv.html)         |
| `transform_select_columns`     | Transform           | This function selects output required columns only. If `type` = 'selected' then only provided columns will be in output (default). If `type` = 'other' then all other columns compare to provided ones will be in output. | `df` DataFrame, `columns` list[str], `type` str            | DataFrame | [PySpark docs](https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.DataFrame.select.html)                         |
| `transform_filter_isin_source` | Transform           | This function is to filter data based on list of values.                                                                                                                                                                  | `df` DataFrame, `column` Column, `criteria` list           | DataFrame | [PySpark docs](https://spark.apache.org/docs/3.1.3/api/python/reference/api/pyspark.sql.Column.isin.html)                              |
| `tranform_join_sources`      | Transform           | This function is joining 2 tables using inner method.                                                                                                                                                                     | `df1` DataFrame, `df2` DataFrame, `condition` list[Column] | DataFrame | [PySpark docs](https://spark.apache.org/docs/3.1.2/api/python/reference/api/pyspark.sql.DataFrame.join.html)                           |
| `transform_rename_columns`     | Transform           | This function is renaming given columns.                                                                                                                                                                                  | `df` DataFrame, `mapping` dict                             | DataFrame | [PySpark docs](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.withColumnRenamed.html) |
| `load_export_dataframe`   | Load                | This function saves DataFrame to given location.                                                                                                                                                                          | `df` DataFrame, `path` str                                 | csv file  | [PySpark docs](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameWriter.save.html)        |
| `job_bitcoin_datamart`   | Job                 | This is combined function which extract 2 files, transformed (select, rename, join) and save it as csv file.                                                                                                           | `input_param` dict                                         | csv file  | `extract_source_csv`, `transform_select_columns`, `transform_filter_isin_source`, `transform_merge_sources`,  `transform_rename_columns`, `load_export_dataframe`               |

## `etl_setup` module

### `SetupKommatiPara` class

| Function                | Transformation type | Description                                                  | Input        | Output       |
| ----------------------- | ------------------- | ------------------------------------------------------------ | ------------ | ------------ |
| `control_spark_session_init`  | Control             | This function establishes new or get existing Spark session. | None         | SparkSession |
| `control_spark_session_close` | Control             | This function terminates existing Spark session.             | SparkSession | None         |
| `control_logging_setup`       | Control             | This function sets up logging configuration.                 | `level` str, `job` str    | None         |

## `arguments_parser` module

### `ParserArguments` class

| Function           | Transformation type | Description                                                         | Input | Output                                                                                          |
| ------------------ | ------------------- | ------------------------------------------------------------------- | ----- | ----------------------------------------------------------------------------------------------- |
| `arguments_job_bitcoin_dm` | Bash arguments      | This function sets up bash script arguments for j_bitcoin_datamart. | None  | dict{`customer`: str, `transactions`: str, `country_flags`: str, `location`: str, `debug`: str} |

