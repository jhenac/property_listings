import pandas as pd

class HubzuCleaner:

    def __init__(self, input_path: str, output_path:str, start_date: str, end_date: str, initial_cols: list, final_cols: list):
        """
        Initializes the HubzuCleaner with file paths and date range.
        """
        self.input_path = input_path
        self.output_path = output_path
        self.start_date = start_date
        self.end_date = end_date
        self.initial_cols = initial_cols
        self.final_cols = final_cols

        try:
            self.df = pd.read_csv(self.input_path)
        except Exception as e:
            raise RuntimeError(f"Failed to read input CSV file {e}")

    def check_missing_columns(self):   
        """
        Check for missing initial columns.
        """
        missing_cols = [col for col in self.initial_cols if col not in self.df.columns]
        if missing_cols:
            raise ValueError(f"Missing required column(s): {','.join(missing_cols)}. Aborting.")
    
    def drop_invalid_rows(self):
       """
       Drops rows where Address or Date columns are missing.
       """
       self.df = self.df.dropna(subset=['Address', 'Date'])

    def split_date_time(self):
        """
        Splits combined Date and Time field, cleans and converts 'Date' to datetime.
        """
        self.df['Date'] = self.df['Date'].str.replace('at', ',', regex=True)
        self.df[['Date', 'Time']] = self.df['Date'].str.split(',', n=1, expand=True)
        self.df['Time'] = self.df['Time'].str.replace('local', '', regex=True)
        self.df['Date'] = pd.to_datetime(self.df['Date'], errors='coerce')
        self.df = self.df.dropna(subset=['Date'])

    def remove_invalid_dates(self):
        """
        Filters rows within specified date range.
        """
        self.start_date = pd.to_datetime(self.start_date)
        self.end_date = pd.to_datetime(self.end_date)
        self.df = self.df[self.df['Date'] >= self.start_date]
        self.df = self.df[self.df['Date'] <= self.end_date]

    def split_address2(self):
        """
        Creates City, State, and Postal columns.
        """
        self.df[['City', 'StateZip']] = self.df['Address2'].str.split(',', n=1, expand=True)
        self.df['StateZip'] = self.df['StateZip'].str.strip()
        self.df[['State', 'Postal']] = self.df['StateZip'].str.split(r'\s+', n=1, expand=True)

    def clean_case(self):
        """
        Removes invalid cases.
        """
        keywords = ['ST', 'WY', 'DR']
        pattern = '|'.join(keywords)
        self.df['Case'] = self.df['Case'].str.upper()
        self.df['Case'] = self.df['Case'].where(~self.df['Case'].isin(self.df['Address']), '')
        self.df['Case'] = self.df['Case'].where(~self.df['Case'].str.contains(pattern, case=False, na=False), '')

    def finalize(self):
        """
        Creates the final csv.
        """
        self.df = self.df.drop_duplicates(subset=['Address'])
        self.df = self.df.reindex(columns=self.final_cols)
        self.df.to_csv(self.output_path, index=False)
    
    def run(self):
        """
        Runs the full cleaning pipeline.
        """
        self.check_missing_columns()
        self.drop_invalid_rows()
        self.split_date_time()
        self.remove_invalid_dates()
        self.split_address2()
        self.clean_case()
        self.finalize()