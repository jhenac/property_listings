import pandas as pd

class AuctionCleaner:

    def __init__(self, input_path: str, output_path: str, start_date: str, end_date: str, minimum_value: str, maximum_value: str, initial_cols: list, final_cols: list):
        """
        Initializes the cleaner with file paths, price range and date range.
        """
        self.input_path = input_path
        self.output_path = output_path
        self.start_date = start_date
        self.end_date = end_date
        self.minimum_value = minimum_value
        self.minimum_value = maximum_value
        self.initial_cols = initial_cols
        self.final_cols = final_cols

        try:
            self.df = pd.read_csv(self.input_path, encoding="latin-1")
        except Exception as e:
            raise RuntimeError(f"Failed to read input CSV file {e}")
        
        def check_missing_columns(self):   
            """
            Check for missing initial columns.
            """
        missing_cols = [col for col in self.initial_cols if col not in self.df.columns]
        if missing_cols:
            raise ValueError(f"Missing required column(s): {','.join(missing_cols)}. Aborting.")

    def split_address2(self):
        """
        Creates city, state, and postal columns.
        """
        self.df['Address3'] = self.df['Address2'].str.replace(r',,+', ',', regex=True)
        self.df['City'] = self.df['Address3'].str.split(',').str[-3]
        self.df['StateZip'] = self.df['Address3'].str.split(',').str[-2].str.strip()
        self.df[['State', 'Postal']] = self.df['StateZip'].str.split(' ', expand=True)
        self.df['County'] = self.df['Address3'].str.split(',').str[-1]
        self.df['City'] = self.df['City'].str.replace(r'\s*\(.*', '', regex=True)

    def atty_contact(self):
        """
        Creates a column for the contact number of foreclosure attorneys.
        """
        self.df['Atty_Contact'] = self.df['Atty_Contact'].str.split('call', n=1).str[1].str.strip()

    def clean_address(self):
        """"
        Removes unnecesarry phrases from Address.
        """
        pattern = r'(?<=\s)(aka|a/k/a|fka|f/k/a|nka|n/k/a|arta)(?=\s)'
        self.df['Address'] = self.df['Address'].str.replace(pattern, ',', regex=True)
        self.df['Address'] = self.df['Address'].str.split(',').str[0]

    def filter_price(self):
        """
        Ensures values are within price range.
        """
        self.df['Value'] = self.df['Value'].str.replace('$', '', regex=True)
        self.df['Value'] = self.df['Value'].str.replace(',', '', regex=True)
        self.df['Value'] = pd.to_numeric(self.df['Value'], errors='coerce')
        self.df = self.df[(self.df['Value'].isna()) | (self.df['Value'] >= 100000)]
        self.df = self.df[(self.df['Value'].isna()) | (self.df['Value'] <= 700000)]

    def create_date_time(self):
        """
        Creates date and time columns.
        """
        self.df['Date'] = self.df['Date'].str.split('\n').str[0]
        self.df['Date'] = pd.to_datetime(self.df['Date'], errors='coerce')
        self.df['Date'] = self.df['Date'].fillna(self.df['Date2'])

        self.df['Time2'] = self.df['Time2'].str.split('-').str[0]
        self.df['Time'] = self.df['Time'].str.replace('TBD', '', regex=True)
        self.df['Time'] = self.df['Time'].fillna(self.df['Time2'])

    def remove_invalid_dates(self):
        """
        Filters rows within specified date range.
        """
        self.df = self.df.dropna(subset='Date')
        start_date = pd.to_datetime(self.start_date)
        end_date = pd.to_datetime(self.end_date)
        self.df = self.df[self.df['Date'] >= start_date]
        self.df = self.df[self.df['Date'] <= end_date]

    def finalize(self):
        """
        Creates the final csv.
        """
        self.df = self.df.reindex(columns=self.final_cols)
        self.df.to_csv(self.output_path, index=False)

    def run(self):
        """
        Runs the full cleaning pipeline.
        """
        self.split_address2()
        self.atty_contact()
        self.clean_address()
        self.filter_price()
        self.create_date_time()
        self.remove_invalid_dates()
        self.finalize()
