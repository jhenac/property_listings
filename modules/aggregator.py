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
        replace_map = {
        '.': '',
        ',': '',
        '#': '',
        '& ': '',
        '-': ' '
    }

        for old, new in replace_map.items():
            self.df['Address'] = self.df['Address'].str.replace(old, new, regex=False)

    def abbreviate_address(self):
        """"
        Abbreviate common address keywords.
        """
        replace_map = {
        'Street': 'St',
        'Drive': 'Dr',
        'Road': 'Rd',
        'Lane': 'Ln',
        'Avenue': 'Ave',
        'Terrace': 'Ter',
        'Circle': 'Cir',
        'Court': 'Ct',
        'Place': 'Pl',
        'Boulevard': 'Blvd',
        'Parkway': 'Pkwy',
        'Ridge': 'Rdg',
        'Trail': 'Trl',
        'Northeast': 'NE',
        'Northwest': 'NW',
        'Southeast': 'SE',
        'Southwest': 'SW',
        'North': 'N',
        'South': 'S',
        'East': 'E',
        'West': 'W'
    }

        for full, abbr in replace_map.items():
            self.df['Address'] = self.df['Address'].str.replace(full, abbr, regex=False)

    def capitalize_atty(self):
        """
        Capitalize common terms in Atty column.
        """
        replace_map = {
        'Llc': 'LLC',
        'llc': 'LLC',
        'Llp': 'LLP',
        'Pc': 'PC'
    }

        for old, new in replace_map.items():
            self.df['Atty'] = self.df['Atty'].str.replace(old, new, regex=False)

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
