from source.arguments_parser import ParserArguments
from source.etl_kommati_para import JobKommatiPara

if __name__ == "__main__":
    args = ParserArguments()
    args_j1 = args.arguments_job_bitcoin_dm()
    logging = 'debug' if args_j1['debug'] else 'info'
    j1 = JobKommatiPara(logging)
    j1.job_bitcoin_datamart(args_j1)
