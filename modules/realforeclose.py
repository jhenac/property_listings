import pandas as pd

class RfcCleaner:
        
    def __init__(self, input_path: str, output_path:str, minimum_value: int, maximum_value: int, initial_cols: list, final_cols: list):
        """
        Initializes the cleaner with file paths and price range.
        """
        self.input_path = input_path
        self.output_path = output_path
        self.minimum_value = minimum_value
        self.maximum_value = maximum_value
        self.initial_cols = initial_cols
        self.final_cols = final_cols
        self.df = pd.read_csv(self.input_path)

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

    def split_date_time(self):
        """
        Splits combined Date and Time field, cleans and converts 'Date' to datetime.
        """
        self.df['Date'] = self.df['Date'].str.replace('\n', '', regex=True)
        self.df['Date'] = self.df['Date'].str.replace('Auction Starts', '', regex=True)
        self.df[['Date', 'Time']] = self.df['Date'].str.split(' ', n=1, expand=True)

    def filter_judgment(self):
        """
        Filters judgment value above 20K.
        """
        self.df['Judgment'] = self.df['Judgment'].str.replace('$', '', regex=True)
        self.df['Judgment'] = self.df['Judgment'].str.replace(',', '', regex=True)
        self.df['Judgment'] = pd.to_numeric(self.df['Judgment'], errors='coerce')
        self.df = self.df[(self.df['Judgment'].isna()) | (self.df['Judgment'] >= 20000)]

    def remove_timeshare(self):
        """
        Removes rows with Timeshare in parcel column.
        """
        self.df = self.df[~self.df['Parcel'].str.contains('TIMESHARE', case=False, na=False)]

    def filter_price(self):
        """
        Filters sale values based on specified price range.
        """
        self.df['Value'] = self.df['Value'].str.replace('$', '', regex=True)
        self.df['Value'] = self.df['Value'].str.replace(',', '', regex=True)
        self.df['Value'] = pd.to_numeric(self.df['Value'], errors='coerce')
        self.df = self.df[(self.df['Value'].isna()) | (self.df['Value'] >= self.minimum_value)]
        self.df = self.df[(self.df['Value'].isna()) | (self.df['Value'] <= self.maximum_value)]

    def extract_location(self):
        """
        Identifies the City, State, Postal Code, and County.
        """
        self.df[['City', 'Postal']] = self.df['Address2'].str.split(',', expand=True)
        self.df['Postal'] = self.df['Postal'].str.replace('NO ZIP', '', regex=True)
        self.df.loc[self.df['Postal'].str.contains('CO', case=False, na=False), 'State'] = 'CO'
        self.df.loc[self.df['Postal'].str.contains('FL', case=False, na=False), 'State'] = 'FL'
        self.df.loc[self.df['URL'].str.contains('ohio', case=False, na=False), 'State'] = 'OH'
        self.df.loc[self.df['URL'].str.contains('pa.', case=False, na=False), 'State'] = 'PA'
        self.df['State'] = self.df['State'].fillna('FL')
        self.df['County'] = self.df['URL'].str.extract(r'//([^\.]+)')
        self.df['Postal'] = self.df['Postal'].str.replace('CO-', '')
        self.df['Postal'] = self.df['Postal'].str.replace('FL-', '')
    
    def finalize(self):
        self.df = self.df.drop_duplicates(subset=['Address'])
        self.df = self.df.reindex(columns=self.final_cols)
        self.df.to_csv(self.output_path, index=False)

    def run(self):
        self.check_missing_columns()
        self.split_date_time()
        self.filter_judgment()
        self.remove_timeshare()
        self.filter_price()
        self.extract_location()
        self.finalize()