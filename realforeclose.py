import pandas as pd
df = pd.read_csv('./input/RFC.csv')

# Splits date into date & time
df['Date'] = df['Date'].str.replace('\n', '')
df['Date'] = df['Date'].str.replace('Auction Starts', '')
df[['Date', 'Time']] = df['Date'].str.split(' ', n=1, expand=True)

# Filters judgment value above 20K
df['Judgment'] = df['Judgment'].str.replace('$', '')
df['Judgment'] = df['Judgment'].str.replace(',', '')
df['Judgment'] = pd.to_numeric(df['Judgment'], errors='coerce')
df = df[(df['Judgment'].isna()) | (df['Judgment'] >= 20000)]

# Removes rows with Timeshare
# Splits Address2 into city, state and postal
df = df[~df['Parcel'].str.contains('TIMESHARE', case=False, na=False)]
df[['City', 'Postal']] = df['Address2'].str.split(',', expand=True)
df['Postal'] = df['Postal'].str.replace('NO ZIP', '')

# Filters values >= 100K but <= 700K
df['Value'] = df['Value'].str.replace('$', '')
df['Value'] = df['Value'].str.replace(',', '')
df['Value'] = pd.to_numeric(df['Value'], errors='coerce')
df = df[(df['Value'].isna()) | (df['Value'] >= 100000)]
df = df[(df['Value'].isna()) | (df['Value'] <= 700000)]

# Identifies the states
df.loc[df['Postal'].str.contains('CO', case=False, na=False), 'State'] = 'CO'
df.loc[df['Postal'].str.contains('FL', case=False, na=False), 'State'] = 'FL'
df.loc[df['URL'].str.contains('ohio', case=False, na=False), 'State'] = 'OH'
df.loc[df['URL'].str.contains('pa.', case=False, na=False), 'State'] = 'PA'
df['State'] = df['State'].fillna('FL')
df['County'] = df['URL'].str.extract(r'//([^\.]+)')
df['Postal'] = df['Postal'].str.replace('CO-', '')
df['Postal'] = df['Postal'].str.replace('FL-', '')

df = df.drop_duplicates(subset=['Address']) #Removes duplicate address

#Creates clean csv with proper column order
cols = ['Address', 'City', 'State', 'Postal', 'County', 'Date', 'Time', 'Case', 'URL', 'Source', 'Atty', 'Atty_Contact']
df = df.reindex(columns=cols)
df.to_csv('./output/RFC2.csv', index=False)