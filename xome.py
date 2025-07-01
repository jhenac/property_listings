import pandas as pd
df = pd.read_csv('./input/Xome.csv')

# Creates the county column
county_cols = ['Text', 'Text1', 'Text2', 'Text3', 'Text4', 'Text5', 'Text6', 'Text7', 'Text8', 'Text9']
df['County'] = df.apply(lambda row: next((str(row[col]) for col in county_cols if 'County' in str(row[col])), None), axis=1)
df = df.drop(['Text', 'Text1', 'Text2', 'Text3', 'Text4', 'Text5', 'Text6', 'Text7', 'Text8', 'Text9'], axis=1)
df['County'] = df['County'].str.replace(r'\s+', '', regex=True) #removes newlines and whitespaces
df['County'] = df['County'].str.replace('MunicipalityCounty:', '')

# Splits address2 to city, state and postal
df['City'] = df['Address2'].str.rsplit(" ", n=2).str[0]
df['State'] = df['Address2'].str.split(" ").str[-2]
df['Postal'] = df['Address2'].str.split(" ").str[-1]

# Splits date into date & time
df = df.dropna(subset='Date')
df['Date'] = df['Date'].str.replace(r'\s+', '', regex=True) #removes newlines and whitespaces
df['Date'] = df['Date'].str.split('-').str[0]
df[['Date', 'Time']] = df['Date'].str.split(',', expand=True)

# Parses date into correct format and filters within date range
start_date = pd.to_datetime('2025-07-02')
end_date = pd.to_datetime('2025-08-02')
df['date_fixed'] = df['Date'].str.replace(r'([a-zA-Z]+)(\d+)', r'\1 \2', regex=True)
df['date_with_year'] = df['date_fixed'] + ' 2025'
df['Date'] = pd.to_datetime(df['date_with_year'], format='%B %d %Y', errors='coerce')
df = df[df['Date'] >= start_date]
df = df[df['Date'] <= end_date]

df = df.drop_duplicates(subset=['Address']) #Removes duplicate address

# Creates clean csv with proper column order
cols = ['Address', 'City', 'State', 'Postal', 'County', 'Date', 'Time', 'Case', 'URL', 'Source', 'Atty', 'Atty_Contact']
df = df.reindex(columns=cols)
df.to_csv('./output/Xome2.csv', index=False)