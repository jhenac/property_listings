import pandas as pd
import glob

files_paths = glob.glob('./output/*2.csv')
dfs = [pd.read_csv(path) for path in files_paths]
df = pd.concat(dfs, ignore_index=True)

# Convert to title case
df.Address = df.Address.str.title()
df.City = df.City.str.title()
df.County = df.County.str.title()
df.Atty = df.Atty.str.title()

# Limit postal code to first 5 digits
df['Postal'] = df['Postal'].astype(str)
df.Postal = df.Postal.str[:5]

# Replace/remove symbols
df.Address = df.Address.str.replace('.', '')
df.Address = df.Address.str.replace(',', '')
df.Address = df.Address.str.replace('#', '')
df.Address = df.Address.str.replace('& ', '')
df.Address = df.Address.str.replace('-', ' ')

# Abbreviate
df.Address = df.Address.str.replace('Street', 'St')
df.Address = df.Address.str.replace('Drive', 'Dr')
df.Address = df.Address.str.replace('Road', 'Rd')
df.Address = df.Address.str.replace('Lane', 'Ln')
df.Address = df.Address.str.replace('Avenue', 'Ave')
df.Address = df.Address.str.replace('Terrace', 'Ter')
df.Address = df.Address.str.replace('Circle', 'Cir')
df.Address = df.Address.str.replace('Court', 'Ct')
df.Address = df.Address.str.replace('Place', 'Pl')
df.Address = df.Address.str.replace('Boulevard', 'Blvd')
df.Address = df.Address.str.replace('Parkway', 'Pkwy')
df.Address = df.Address.str.replace('Ridge', 'Rdg')
df.Address = df.Address.str.replace('Trail', 'Trl')
df.Address = df.Address.str.replace('North', 'N')
df.Address = df.Address.str.replace('Northeast', 'NE')
df.Address = df.Address.str.replace('Northwest', 'NW')
df.Address = df.Address.str.replace('South', 'S')
df.Address = df.Address.str.replace('Southeast', 'SE')
df.Address = df.Address.str.replace('Southwest', 'SW')
df.Address = df.Address.str.replace('East', 'E')
df.Address = df.Address.str.replace('West', 'W')

# Capitalize common terms in Atty column
df.Atty = df.Atty.str.replace('Llc', 'LLC')
df.Atty = df.Atty.str.replace('llc', 'LLC')
df.Atty = df.Atty.str.replace('Llp', 'LLP')
df.Atty = df.Atty.str.replace('Pc', 'PC')

# Clean County column
df.County = df.County.str.replace('Myorangeclerk', 'My Orange Clerk')
df.County = df.County.str.replace('County', '')

# Removes duplicates in Address but fill null values with values of duplicate rows prior to dropping
df = df.groupby('Address', as_index=False).first()

# Create new clean csv
df.to_csv('Clean.csv', index=False)
