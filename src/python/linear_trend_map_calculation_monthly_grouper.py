import pandas as pd


soil_state = 'Thawed'
csv_file_path_save = r"./../../output/data/csv/trend_analysis/linear_{}_trend_mk_monthly.csv".format(soil_state)
df = pd.read_csv(csv_file_path_save)
print(df.head())


# Group by 'Month' and create a separate DataFrame for each group
grouped_dfs = {month: group for month, group in df.groupby('Month')}


# If you need to save each DataFrame to a CSV file, you can do so with a loop:
for month, group_df in grouped_dfs.items():
    filename = r"./../../output/data/csv/trend_analysis/linear_{}_trend_mk_{}.csv".format(soil_state,str(int(month)))  # Customize the filename as needed
    group_df.to_csv(filename, index=False)
    print(f"Saved {filename}")
