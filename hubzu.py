import pandas as pd
df = pd.read_csv('Hubzu.csv')

df = df.dropna(subset=['Address', 'Date']) #drop null values from Address & Date

#Splits date to date & time
df.Date = df.Date.str.replace('at', ',')
df[['Date', 'Time']] = df['Date'].str.split(',', n=1, expand=True)
df.Time = df.Time.str.replace('local', '')
df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
df = df.dropna(subset=['Date'])

#Keeps valid date range
start_date = pd.to_datetime('2025-07-02')
end_date = pd.to_datetime('2025-08-02')
df = df[df['Date'] >= start_date]
df = df[df['Date'] <= end_date]
df = df.sort_values(by='Date')

#Splits address2 to city, state * postal
df[['City', 'Text2']] = df['Address2'].str.split(',', n=1, expand=True)
df[['Blank', 'State', 'Postal']] = df['Text2'].str.split('  ', n=2, expand=True)
df = df.drop(['Address2','Text2' , 'Blank'], axis=1)

#Drops address in case column
keywords = ['ST', 'WY', 'DR']
pattern = '|'.join(keywords)
df.Case = df.Case.str.upper()
df['Case'] = df['Case'].where(~df['Case'].isin(df['Address']), '')
df['Case'] = df['Case'].where(~df['Case'].str.contains(pattern, case=False, na=False), '')

df = df.drop_duplicates(subset=['Address']) #Removes duplicate address

#Create the clean csv with correct column order
cols = ['Address', 'City', 'State', 'Postal', 'County', 'Date', 'Time', 'Case', 'URL', 'Source', 'Atty', 'Atty_Contact']
df = df.reindex(columns=cols)
df.to_csv('Hubzu2.csv', index=False)