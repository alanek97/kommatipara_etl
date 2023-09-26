import argparse
class BashArguments():
    def arg_j_bitcoin_dm(self) -> dict:
        '''
        Type of method: Bash arguments

        This function sets up bash script arguments for j_bitcoin_datamart.

        input: None
        output: dict{'customer': str, 'transations': str, 'country_flags': str, 'location': str, 'debbug': str}
        '''
        parser = argparse.ArgumentParser(
            prog='j_bitcoin_datamart',
            description='ETL program for bitcoin datamart creation. Exctract 2 files, combine and save it.')

        parser.add_argument('-c', '--customer' , help = 'path for customer file', type=str, required=True)
        parser.add_argument('-t', '--transations', help = 'path for transation file', type=str, required=True)
        parser.add_argument('-f', '--country_flags', help = 'Country names for filter', nargs='+', required=True)
        parser.add_argument('-l', '--location', help = 'path for output csv file', type=str, required=False, default='.')
        parser.add_argument('-d', '--debbug', help = 'Debbug - set up more detail logging messages', type=bool, required=False, default=False, action=argparse.BooleanOptionalAction) 

        args = parser.parse_args()
        return vars(args)