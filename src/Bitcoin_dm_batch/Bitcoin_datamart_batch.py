
from src.ETL_source_code.ETL_kommatiPara import ETLKommatiPara
from src.Batch_arguments.batchArguments_kommatiPara import batchArguments

if __name__ == "__main__":
    args = batchArguments()
    args_j1 = args.arg_j_bitcoin_dm()
    j1 = ETLKommatiPara()
    j1.j_bitcoin_datamart(args_j1)