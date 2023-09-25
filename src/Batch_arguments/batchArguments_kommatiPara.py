
import argparse
class batchArguments():
    def arg_j_bitcoin_dm(self) -> dict:
        parser = argparse.ArgumentParser(
            prog='j_bitcoin_datamart',
            description='ETL program for bitcoin datamart creation. Exctract 2 files, combine and save it.')

        parser.add_argument('-c', '--customer' , help = 'path for customer file', type=str, required=True)
        parser.add_argument('-t', '--transations', help = 'path for transation file', type=str, required=True)
        parser.add_argument('-f', '--country_flags', help = 'Country names for filter', nargs='+', required=True)

        args = parser.parse_args()
        return vars(args)