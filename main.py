from modules.hubzu import HubzuCleaner
from modules.auction import AuctionCleaner
from modules.realforeclose import RfcCleaner
from modules.xome import XomeCleaner
from modules.aggregator import Aggregator

input_dir = './input/'
input_files = {
    'Hubzu': 'Hubzu.csv',
    'Auction': 'Auction.csv',
    'RFC': 'RFC.csv',
    'Xome': 'Xome.csv'
}


start_date = '2025-07-09'
end_date = '2025-08-09'
minimum_value = 100000
maximum_value = 700000
initial_columns = ['Address', 'Address2', 'Date', 'Case', 'URL', 'Source', 'Atty', 'Atty_Contact']
final_columns = ['Address', 'City', 'State', 'Postal', 'County', 'Date', 'Time', 'Case', 'URL', 'Source', 'Atty', 'Atty_Contact']

hubzu_cleaner = HubzuCleaner(
    input_path=f"{input_dir}{input_files['Hubzu']}",
    start_date=start_date,
    end_date=end_date,
    initial_cols = initial_columns,
    final_cols=final_columns
)

auction_cleaner = AuctionCleaner(
    input_path=f"{input_dir}{input_files['Auction']}",
    start_date=start_date,
    end_date=end_date,
    minimum_value=minimum_value,
    maximum_value=maximum_value,
    initial_cols=initial_columns,
    final_cols=final_columns
)


rfc_cleaner = RfcCleaner(
    input_path=f"{input_dir}{input_files['RFC']}",
    minimum_value=minimum_value,
    maximum_value=maximum_value,
    initial_cols=initial_columns,
    final_cols=final_columns
)


xome_cleaner = XomeCleaner(
    input_path=f"{input_dir}{input_files['Xome']}",
    start_date=start_date,
    end_date=end_date,
    initial_cols=initial_columns,
    final_cols=final_columns
)

hubzu_df = hubzu_cleaner.run()
auction_df = auction_cleaner.run()
realforeclose_df = rfc_cleaner.run()
xome_df = xome_cleaner.run()

aggregator = Aggregator(
    realforeclose=realforeclose_df,
    xome=xome_df,
    auction=auction_df,
    hubzu=hubzu_df
)

df = aggregator.aggregate()
df.to_csv('Clean.csv', index=False)



