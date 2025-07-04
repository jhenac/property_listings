import pandas as pd
from datetime import datetime

class XomeCleaner:
    
    def __init__(self, input_path: str, start_date: str, end_date: str, initial_cols: list, final_cols: list):
        """
        Initializes the cleaner with file paths and date range.
        """
        self.input_path = input_path
        self.start_date = pd.to_datetime(start_date)
        self.end_date = pd.to_datetime(end_date)
        self.initial_cols = initial_cols
        self.final_cols = final_cols

        try:
            self.df = pd.read_csv(self.input_path)
        except Exception as e:
            raise RuntimeError(f"Failed to read input CSV file at '{self.input_path}' {e}")

        self.df.columns = self.df.columns.str.strip()

    def check_missing_columns(self):   
        """
        Check for missing initial columns.
        """
        missing_cols = [col for col in self.initial_cols if col not in self.df.columns]
        if missing_cols:
            raise ValueError(f"Missing required column(s): {','.join(missing_cols)}. Aborting.")

    def create_county(self):
        """
        Creates the county column.
        """
        county_cols = ['Text', 'Text1', 'Text2', 'Text3', 'Text4', 'Text5', 'Text6', 'Text7', 'Text8', 'Text9']
        self.df['County'] = self.df.apply(
            lambda row: next((str(row[col]) for col in county_cols if 'County' in str(row[col])), None), axis=1
            )
        self.df['County'] = self.df['County'].str.replace(r'\s+', '', regex=True)
        self.df['County'] = self.df['County'].str.replace('MunicipalityCounty:', '', regex=True)

    def split_address2(self):
        """"
        Splits Address2 to City, State and Postal Code.
        """
        self.df['City'] = self.df['Address2'].str.rsplit(' ', n=2).str[0]
        self.df['State'] = self.df['Address2'].str.split(' ').str[-2]
        self.df['Postal'] = self.df['Address2'].str.split(' ').str[-1]

    def format_date_time(self):
        """
        Creates Date and Time columns and filters date within range.
        """
        current_year = datetime.now().year
        self.df = self.df.dropna(subset=['Date'])
        self.df['Date'] = self.df['Date'].str.replace(r'\s+', '', regex=True)
        self.df['Date'] = self.df['Date'].str.split('-').str[0]
        self.df[['Date', 'Time']] = self.df['Date'].str.split(',', expand=True)
        self.df['date_fixed'] = self.df['Date'].str.replace(r'([a-zA-Z]+)(\d+)', r'\1 \2', regex=True)
        self.df['date_with_year'] = self.df['date_fixed'] + f' {current_year}'
        self.df['Date'] = pd.to_datetime(self.df['date_with_year'], format='%B %d %Y', errors='coerce')
        self.df = self.df[self.df['Date'] >= self.start_date]
        self.df = self.df[self.df['Date'] <= self.end_date]

    def finalize(self):
        """
        Creates the final csv.
        """
        self.df = self.df.drop_duplicates(subset=['Address'])
        self.df = self.df.reindex(columns=self.final_cols)
        return self.df

    def run(self):
        self.check_missing_columns()
        self.create_county()
        self.split_address2()
        self.format_date_time()
        return self.finalize()