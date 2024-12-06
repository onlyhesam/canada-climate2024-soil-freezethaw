import pandas as pd
import matplotlib.pyplot as plt 
import ast  # To safely evaluate the string representation of tuples
import numpy as np 
from scipy.stats import linregress
import pymannkendall as mk


csv_file_path = r"./../../output/data/csv/general/number_of_thawed_frozen_trans_day_per_year.csv"
df = pd.read_csv(csv_file_path, header=0, names=['Grid', 'Year', 'Frozen', 'Thawed', 'Transition_0', 'Transition_1'])
df['Transition'] = df.Transition_0 + df.Transition_1
df.drop(columns=['Transition_0', 'Transition_1'], inplace=True)


#################### 
# 0 = AM/PM frozen # 1 = AM/PM thawed # 2 = AM frozen, PM thawed (transitional) # 3 = AM thawed, PM frozen (inverse transitional)
#################### 


long_lat= pd.read_csv(r'.\..\..\output\data\csv\sample\selected_data.csv')
long_lat = long_lat.loc[:,['longitude', 'latitude', 'original_indices', 'landcover']]
long_lat.rename(columns={'original_indices':'Grid'}, inplace=True)


main = pd.merge(df, long_lat, on='Grid', how='outer')


soil_state = 'Thawed'
# Assuming df is your DataFrame with columns: 'grid', 'frozen', 'year'
# Function to apply linear regression to each group
def apply_regression(group):
    # Performing linear regression
    slope, intercept, r_value, p_value, std_err = linregress(group['Year'], group['{}'.format(soil_state)])
    # Performing Mann-Kendall test
    mk_result = mk.original_test(group['{}'.format(soil_state)])

    
    # Constructing a Series to include various results from MK test and regression
    results = pd.Series({
        'm': slope,
        'b': intercept,
        'r2': r_value,
        'std_err': std_err,
        'p_value': p_value,
        'MK_trend': mk_result.trend,
        'MK_p_value': mk_result.p,
        'MK_z': mk_result.z,
        'MK_tau': mk_result.Tau,
        'MK_s': mk_result.s,
        'MK_var_s': mk_result.var_s,
        'MK_h': mk_result.h
    })

    return results


# Grouping by 'grid' and applying the regression function
regression_results = df.groupby('Grid').apply(apply_regression).reset_index()

# regression_results now has the columns 'grid', 'm', and 'b'

transition_trend = pd.merge(regression_results, long_lat, on='Grid', how='outer')
transition_trend = transition_trend.dropna()


csv_file_path = r"./../../output/data/csv/trend_analysis/linear_{}_trend_mk_annual.csv".format(soil_state)
transition_trend.to_csv(csv_file_path)



df = pd.read_csv(csv_file_path)
df = df[df.MK_p_value <= 0.05]
csv_file_path_save = r"./../../output/data/csv/trend_analysis/linear_{}_trend_mk_annual_ready_for_mapping.csv".format(soil_state)
df.to_csv(csv_file_path_save)