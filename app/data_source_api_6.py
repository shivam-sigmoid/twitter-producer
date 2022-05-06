import pandas as pd


df = pd.read_excel("../data/Donations.xlsx",sheet_name="Overall")

# for k,v in df.items():
#     print(k)
#
# gdf = df.groupby('Donor')
#
# print(gdf.head())

print(df.head())