import pandas as pd
df = pd.read_csv('./input/Auction.csv', encoding="latin-1")
df = df.dropna(subset='Date') #Removes null dates

# Creates city, state, and postal columns
df['Address3'] = df['Address2'].str.replace(r',,+', ',', regex=True)
df['City'] = df['Address3'].str.split(',').str[-3]
df['StateZip'] = df['Address3'].str.split(',').str[-2].str.strip()
df[['State', 'Postal']] = df['StateZip'].str.split(' ', expand=True)
df['County'] = df['Address3'].str.split(',').str[-1]
df['City'] = df['City'].str.replace(r'\s*\(.*', '', regex=True)

# Creates atty_contact column
df['Atty_Contact'] = df['Atty2'].str.split('call', n=1).str[1].str.strip()

# Removes unnecessary keywords from address
df['Address'] = df['Address'].str.replace(' aka ', ',', case=False)
df['Address'] = df['Address'].str.replace(' a/k/a ', ',', case=False)
df['Address'] = df['Address'].str.replace(' fka ', ',', case=False)
df['Address'] = df['Address'].str.replace(' f/k/a ', ',', case=False)
df['Address'] = df['Address'].str.replace(' nka ', ',', case=False)
df['Address'] = df['Address'].str.replace(' n/k/a ', ',', case=False)
df['Address'] = df['Address'].str.replace(' arta ', ',', case=False)
df['Address'] = df['Address'].str.split(',').str[0]

# Filters value >= 100K and <= 700K
df['Value'] = df['Value'].str.replace('$', '')
df['Value'] = df['Value'].str.replace(',', '')
df['Value'] = pd.to_numeric(df['Value'], errors='coerce')
df = df[(df['Value'].isna()) | (df['Value'] >= 100000)]
df = df[(df['Value'].isna()) | (df['Value'] <= 700000)]

# Transforms date and time into their proper format
df['Date'] = df['Date'].str.split('\n').str[0]
df['Date'] = df['Date'].str.split(',', n=1).str[1]
df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
df['Date2'] = df['Time'].apply(
    lambda x: x if isinstance(x, str) and '2025' in x else ''
    )
df['Time'] = df['Time'].apply(
    lambda x: '' if isinstance(x, str) and '2025' in x else x
    )
df['Time'] = df['Time'].str.split('\n').str[0]
df['Time'] = df['Time'].str.replace('TBD', '')
df['Date2'] = df['Date2'].str.split('-').str[0]   
df[['Date3', 'Time2']] = df['Date2'].apply(
    lambda s: pd.Series([' '.join(s.split(' ', 3)[:3]), s.split(' ', 3)[3] if len(s.split(' ', 3)) == 4 else ''])
    )
df['Date'] = df['Date'].fillna(df['Date3'])
df = df.dropna(subset='Date')
df['combined_T'] = df['Time'].fillna('').astype(str) + df['Time2'].fillna('').astype(str)
df = df.drop(['Time'], axis=1)
df.rename(columns={'combined_T': 'Time'}, inplace=True)

# Keeps valid date range
start_date = pd.to_datetime('2025-07-02')
end_date = pd.to_datetime('2025-08-02')
df = df[df['Date'] >= start_date]
df = df[df['Date'] <= end_date]

# Creates clean csv with proper column order
cols = ['Address', 'City', 'State', 'Postal', 'County', 'Date', 'Time', 'Case', 'URL', 'Source', 'Atty', 'Atty_Contact']
df = df.reindex(columns=cols)
df.to_csv('./output/Auction2.csv', index=False)