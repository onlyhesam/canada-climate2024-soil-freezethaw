
import pandas as pd
import glob 
import os

files = glob.glob(r'.\..\..\output\data\csv\trend_analysis\linear_Thawed_trend_mk_annual.csv')


for file in files:
    name = os.path.basename(file)[:-4]
    parts = name.split("_")
    month = parts[-1]
    
    df = pd.read_csv(file)
    df = df[df.MK_p_value > 0.05]
    csv_file_path_save = r"./../../output/data/csv/trend_analysis/linear_Thawed_no_trend_mk_annual.csv".format(month)
    df.to_csv(csv_file_path_save)