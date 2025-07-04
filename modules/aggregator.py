import pandas as pd

class Aggregator:
    
    def __init__(self, realforeclose, xome, auction, hubzu):
        """
        Initialize and combine files.
        """
        self.df = pd.concat([realforeclose, xome, auction, hubzu], ignore_index=True)

    def title_case_format(self):
        """
        Convert Address, City, County, and Atty columns to title case,
        """
        self.df['Address'] = self.df['Address'].str.title()
        self.df['City'] = self.df['City'].str.title()
        self.df['County'] = self.df['County'].str.title()
        self.df['Atty'] = self.df['Atty'].str.title()

    def format_zipcode(self):
        """
        Limit postal code to first 5 digits.
        """
        self.df['Postal'] = self.df['Postal'].astype(str)
        self.df['Postal'] = self.df['Postal'].str[:5]

    def clean_symbols(self):
        """
        Replace or remove symbols from Address.
        """
        self.df['Address'] = self.df['Address'].str.replace('.', '')
        self.df['Address'] = self.df['Address'].str.replace(',', '')
        self.df['Address'] = self.df['Address'].str.replace('#', '')
        self.df['Address'] = self.df['Address'].str.replace('& ', '')
        self.df['Address'] = self.df['Address'].str.replace('-', ' ')

    def abbreviate_address(self):
        """"
        Abbreviate common address keywords.
        """
        self.df['Address'] = self.df['Address'].str.replace('Street', 'St')
        self.df['Address'] = self.df['Address'].str.replace('Drive', 'Dr')
        self.df['Address'] = self.df['Address'].str.replace('Road', 'Rd')
        self.df['Address'] = self.df['Address'].str.replace('Lane', 'Ln')
        self.df['Address'] = self.df['Address'].str.replace('Avenue', 'Ave')
        self.df['Address'] = self.df['Address'].str.replace('Terrace', 'Ter')
        self.df['Address'] = self.df['Address'].str.replace('Circle', 'Cir')
        self.df['Address'] = self.df['Address'].str.replace('Court', 'Ct')
        self.df['Address'] = self.df['Address'].str.replace('Place', 'Pl')
        self.df['Address'] = self.df['Address'].str.replace('Boulevard', 'Blvd')
        self.df['Address'] = self.df['Address'].str.replace('Parkway', 'Pkwy')
        self.df['Address'] = self.df['Address'].str.replace('Ridge', 'Rdg')
        self.df['Address'] = self.df['Address'].str.replace('Trail', 'Trl')
        self.df['Address'] = self.df['Address'].str.replace('North', 'N')
        self.df['Address'] = self.df['Address'].str.replace('Northeast', 'NE')
        self.df['Address'] = self.df['Address'].str.replace('Northwest', 'NW')
        self.df['Address'] = self.df['Address'].str.replace('South', 'S')
        self.df['Address'] = self.df['Address'].str.replace('Southeast', 'SE')
        self.df['Address'] = self.df['Address'].str.replace('Southwest', 'SW')
        self.df['Address'] = self.df['Address'].str.replace('East', 'E')
        self.df['Address'] = self.df['Address'].str.replace('West', 'W')

    def capitalize_atty(self):
        """
        Capitalize common terms in Atty column.
        """
        self.df['Atty'] = self.df['Atty'].str.replace('Llc', 'LLC')
        self.df['Atty'] = self.df['Atty'].str.replace('llc', 'LLC')
        self.df['Atty'] = self.df['Atty'].str.replace('Llp', 'LLP')
        self.df['Atty'] = self.df['Atty'].str.replace('Pc', 'PC')

    def clean_county(self):
        """
        Clean County column.
        """
        self.df['County'] = self.df['County'].str.replace('Myorangeclerk', 'My Orange Clerk')
        self.df['County'] = self.df['County'].str.replace('County', '')

    def finalize(self):
        """
        Creates the final csv file.
        """
        self.df = self.df.groupby('Address', as_index=False).first()
        return self.df
    
    def aggregate(self):
        self.title_case_format()
        self.format_zipcode()
        self.clean_symbols()
        self.abbreviate_address()
        self.capitalize_atty()
        self.clean_county()
        return self.finalize()
