from source.arguments_parser_bitcoin import BashArguments
from source.etl_kommati_para import ETLKommatiPara

if __name__ == "__main__":
    args = BashArguments()
    args_j1 = args.arg_j_bitcoin_dm()
    j1 = ETLKommatiPara()
    j1.debbug = args_j1['debbug']
    j1.j_bitcoin_datamart(args_j1)