# Initialization
import pandas as pd
import numpy as np
rf = pd.read_csv('Station 43 (S18 Patch 3).csv')
sm = pd.read_csv('Station 396.csv')

# Quality filtering for rainfall
rf.loc[rf['Calidad'] == 2512, ['P2']] = float('nan')
rf.loc[rf['Calidad'] == 2511, ['P1']] = float('nan')
rf.loc[rf['Calidad'] == 2510, ['P1', 'P2']] = float('nan')
rf.loc[rf['Calidad'] == 1512, ['P2']] = float('nan')
rf.loc[rf['Calidad'] == 1511, ['P1']] = float('nan')
rf.loc[rf['Calidad'] == 1510, ['P1', 'P2']] = float('nan')
rf.loc[rf['Calidad'] == 251, ['P1', 'P2']] = float('nan')
rf.loc[rf['Calidad'] == 151, ['P1', 'P2']] = float('nan')
rf.loc[rf['P2'] == -999, ['P2']] = float('nan')
rf.loc[rf['P1'] == -999, ['P1']] = float('nan')
del rf['Calidad']

# Quality filtering for soil moisture
sm.loc[sm['calidad'] == 2528, ['h2', 'h3']] = float('nan')
sm.loc[sm['calidad'] == 2527, ['h1', 'h3']] = float('nan')
sm.loc[sm['calidad'] == 2526, ['h1', 'h2']] = float('nan')
sm.loc[sm['calidad'] == 2523, ['h3']] = float('nan')
sm.loc[sm['calidad'] == 2522, ['h2']] = float('nan')
sm.loc[sm['calidad'] == 2521, ['h1']] = float('nan')
sm.loc[sm['calidad'] == 1528, ['h2', 'h3']] = float('nan')
sm.loc[sm['calidad'] == 1527, ['h1', 'h3']] = float('nan')
sm.loc[sm['calidad'] == 1526, ['h1', 'h2']] = float('nan')
sm.loc[sm['calidad'] == 1523, ['h3']] = float('nan')
sm.loc[sm['calidad'] == 1522, ['h2']] = float('nan')
sm.loc[sm['calidad'] == 1521, ['h1']] = float('nan')
sm.loc[sm['calidad'] == 251, ['h1', 'h2', 'h3']] = float('nan')
sm.loc[sm['calidad'] == 151, ['h1', 'h2', 'h3']] = float('nan')
sm.loc[sm['h3'] == -999, ['h3']] = float('nan')
sm.loc[sm['h2'] == -999, ['h2']] = float('nan')
sm.loc[sm['h1'] == -999, ['h1']] = float('nan')
del sm['calidad']

# Choosing maximum rainfall, converting to inches, and cutting HH:MM:SS
rf['RF'] = rf[['P1', 'P2']].max(axis=1)
del rf['P1']
del rf['P2']
rf['RF'] = rf['RF'] / 25.4
rf['RF'] = rf['RF'].round(2)
rf = rf.rename(columns={'fecha_hora': 'Date'})
rf['Date'] = rf['Date'].str[:-9]
rf = rf[(rf['Date'] >= '2020-03-01') & (rf['Date'] <= '2022-03-31')]

# Rounding and splitting VWC data, as well as cutting HH:MM:SS
sm['h1'] = sm['h1'].round(1)
sm['h2'] = sm['h2'].round(1)
sm['h3'] = sm['h3'].round(1)
sm = sm.rename(columns={'h1': 'VWC1'})
sm = sm.rename(columns={'h2': 'VWC2'})
sm = sm.rename(columns={'h3': 'VWC3'})
sm = sm.rename(columns={'fecha_hora': 'Date'})
sm['Date'] = sm['Date'].str[:-9]
sm = sm[(sm['Date'] >= '2020-04-01') & (sm['Date'] <= '2022-03-31')]
sm1 = sm.copy()
sm2 = sm.copy()
sm3 = sm.copy()
sm1 = sm1.drop(columns=['VWC2', 'VWC3'])
sm2 = sm2.drop(columns=['VWC1', 'VWC3'])
sm3 = sm3.drop(columns=['VWC1', 'VWC2'])

# Count entries per day and cut days with less than 75% entries (RF)
rf = rf.dropna(subset=['RF'])
count_rf = rf.groupby('Date')['Date'].count()
remove_rf = count_rf[count_rf < 1080].index.tolist()
rf = rf[~rf['Date'].isin(remove_rf)]

# Count entries per day and cut days with less than 40% entries (VWC)
sm1 = sm1.dropna(subset=['VWC1'])
sm2 = sm2.dropna(subset=['VWC2'])
sm3 = sm3.dropna(subset=['VWC3'])
count_sm1 = sm1.groupby('Date')['Date'].count()
count_sm2 = sm2.groupby('Date')['Date'].count()
count_sm3 = sm3.groupby('Date')['Date'].count()
remove_sm1 = count_sm1[count_sm1 < 576].index.tolist()
remove_sm2 = count_sm2[count_sm2 < 576].index.tolist()
remove_sm3 = count_sm3[count_sm3 < 576].index.tolist()
sm1 = sm1[~sm1['Date'].isin(remove_sm1)]
sm2 = sm2[~sm2['Date'].isin(remove_sm2)]
sm3 = sm3[~sm3['Date'].isin(remove_sm3)]

# Sum rainfall data to get total daily rainfall
rf_grouped = rf.groupby('Date').agg({'RF': 'sum'})
rf_grouped['RF'] = rf_grouped['RF'].round(2)

# Manual fix for rainfall
rf_grouped.loc['2021-09-23', 'RF'] = 1.04

# Average soil moisture data to get average soil moisture
sm1_grouped = sm1.groupby('Date').agg({'VWC1': 'mean'})
sm2_grouped = sm2.groupby('Date').agg({'VWC2': 'mean'})
sm3_grouped = sm3.groupby('Date').agg({'VWC3': 'mean'})
sm1_grouped['VWC1'] = sm1_grouped['VWC1'].round(1)
sm2_grouped['VWC2'] = sm2_grouped['VWC2'].round(1)
sm3_grouped['VWC3'] = sm3_grouped['VWC3'].round(1)

# Data copied from January 2021, which also lacked rain
sm1_grouped.loc['2022-01-24', 'VWC1'] = 43.8
sm1_grouped.loc['2022-01-25', 'VWC1'] = 43.7
sm1_grouped.loc['2022-01-26', 'VWC1'] = 43.5
sm1_grouped.loc['2022-01-27', 'VWC1'] = 43.3
sm1_grouped.loc['2022-01-28', 'VWC1'] = 43.1
sm1_grouped.loc['2022-01-29', 'VWC1'] = 42.9
sm1_grouped.loc['2022-01-30', 'VWC1'] = 42.7
sm1_grouped.loc['2022-01-31', 'VWC1'] = 42.5
sm1_grouped.loc['2022-02-01', 'VWC1'] = 42.2
sm1_grouped.loc['2022-02-02', 'VWC1'] = 42.1
sm2_grouped.loc['2022-01-24', 'VWC2'] = 41.3
sm2_grouped.loc['2022-01-25', 'VWC2'] = 40.9
sm2_grouped.loc['2022-01-26', 'VWC2'] = 40.7
sm2_grouped.loc['2022-01-27', 'VWC2'] = 40.5
sm2_grouped.loc['2022-01-28', 'VWC2'] = 40.2
sm2_grouped.loc['2022-01-29', 'VWC2'] = 39.9
sm2_grouped.loc['2022-01-30', 'VWC2'] = 39.7
sm2_grouped.loc['2022-01-31', 'VWC2'] = 39.5
sm2_grouped.loc['2022-02-01', 'VWC2'] = 39.1
sm2_grouped.loc['2022-02-02', 'VWC2'] = 38.8
sm3_grouped.loc['2022-01-24', 'VWC3'] = 44.4
sm3_grouped.loc['2022-01-25', 'VWC3'] = 43.7
sm3_grouped.loc['2022-01-26', 'VWC3'] = 43.0
sm3_grouped.loc['2022-01-27', 'VWC3'] = 42.3
sm3_grouped.loc['2022-01-28', 'VWC3'] = 41.5
sm3_grouped.loc['2022-01-29', 'VWC3'] = 40.8
sm3_grouped.loc['2022-01-30', 'VWC3'] = 40.3
sm3_grouped.loc['2022-01-31', 'VWC3'] = 40.0
sm3_grouped.loc['2022-02-01', 'VWC3'] = 39.5
sm3_grouped.loc['2022-02-02', 'VWC3'] = 38.7

# Linearly interpolated
sm1_grouped.loc['2020-09-22', 'VWC1'] = 44.6
sm1_grouped.loc['2020-09-23', 'VWC1'] = 44.4
sm1_grouped.loc['2020-09-24', 'VWC1'] = 44.2
sm2_grouped.loc['2020-09-22', 'VWC2'] = 43.7
sm2_grouped.loc['2020-09-23', 'VWC2'] = 43.0
sm2_grouped.loc['2020-09-24', 'VWC2'] = 42.3
sm3_grouped.loc['2020-09-22', 'VWC3'] = 48.7
sm3_grouped.loc['2020-09-23', 'VWC3'] = 48.3
sm3_grouped.loc['2020-09-24', 'VWC3'] = 47.9
sm1_grouped.loc['2021-02-11', 'VWC1'] = 40.4
sm1_grouped.loc['2021-02-12', 'VWC1'] = 40.7
sm1_grouped.loc['2021-02-13', 'VWC1'] = 41.1
sm2_grouped.loc['2021-02-11', 'VWC2'] = 35.9
sm2_grouped.loc['2021-02-12', 'VWC2'] = 36.5
sm2_grouped.loc['2021-02-13', 'VWC2'] = 37.2
sm3_grouped.loc['2021-02-11', 'VWC3'] = 42.2
sm3_grouped.loc['2021-02-12', 'VWC3'] = 43.0
sm3_grouped.loc['2021-02-13', 'VWC3'] = 43.8

# Create a dataframe for every day
ranges = pd.date_range(start='03/01/2020', end='03/31/2022', freq='D')
dates = pd.DataFrame(ranges, columns=['Date'])
dates['Date'] = dates['Date'].dt.strftime('%Y-%m-%d')

# Merge all files
df_merged = pd.merge(dates, rf_grouped, on='Date', how='left')
df_merged = pd.merge(df_merged, sm1_grouped, on='Date', how='left')
df_merged = pd.merge(df_merged, sm2_grouped, on='Date', how='left')
df_merged = pd.merge(df_merged, sm3_grouped, on='Date', how='left')
df_merged['RF'] = round(df_merged['RF'] * 25.4, 3)

# Calculating VWC differences
df_merged['Diff. (VWC1)'] = round(df_merged['VWC1'].diff(), 1)
df_merged['Diff. (VWC2)'] = round(df_merged['VWC2'].diff(), 1)
df_merged['Diff. (VWC3)'] = round(df_merged['VWC3'].diff(), 1)
factor = 0.8
df_merged['ARFY'] = round(df_merged['RF'] + df_merged['RF'].shift(1) * factor, 0)
df_merged['ARF3'] = round(df_merged['RF'] + df_merged['RF'].shift(1) * factor + df_merged['RF'].shift(2) * factor * factor, 0)
df_merged = df_merged[df_merged['Date'] >= '2020-04-01']

# Calculating antecedent rainfall index and checking correlation
threshold = 16
df_merged['RF/VWC1'] = 0
df_merged.loc[((df_merged['RF'] >= threshold) & (df_merged['Diff. (VWC1)'] > 0)) | ((df_merged['RF'] < threshold) & (df_merged['Diff. (VWC1)'] <= 0)), 'RF/VWC1'] = 1
df_merged['RF/VWC2'] = 0
df_merged.loc[((df_merged['RF'] >= threshold) & (df_merged['Diff. (VWC2)'] > 0)) | ((df_merged['RF'] < threshold) & (df_merged['Diff. (VWC2)'] <= 0)), 'RF/VWC2'] = 1
df_merged['RF/VWC3'] = 0
df_merged.loc[((df_merged['RF'] >= threshold) & (df_merged['Diff. (VWC3)'] > 0)) | ((df_merged['RF'] < threshold) & (df_merged['Diff. (VWC3)'] <= 0)), 'RF/VWC3'] = 1
df_merged['ARFY/VWC1'] = 0
df_merged.loc[((df_merged['ARFY'] >= threshold) & (df_merged['Diff. (VWC1)'] > 0)) | ((df_merged['ARFY'] < threshold) & (df_merged['Diff. (VWC1)'] <= 0)), 'ARFY/VWC1'] = 1
df_merged['ARFY/VWC2'] = 0
df_merged.loc[((df_merged['ARFY'] >= threshold) & (df_merged['Diff. (VWC2)'] > 0)) | ((df_merged['ARFY'] < threshold) & (df_merged['Diff. (VWC2)'] <= 0)), 'ARFY/VWC2'] = 1
df_merged['ARFY/VWC3'] = 0
df_merged.loc[((df_merged['ARFY'] >= threshold) & (df_merged['Diff. (VWC3)'] > 0)) | ((df_merged['ARFY'] < threshold) & (df_merged['Diff. (VWC3)'] <= 0)), 'ARFY/VWC3'] = 1
df_merged['ARF3/VWC1'] = 0
df_merged.loc[((df_merged['ARF3'] >= threshold) & (df_merged['Diff. (VWC1)'] > 0)) | ((df_merged['ARF3'] < threshold) & (df_merged['Diff. (VWC1)'] <= 0)), 'ARF3/VWC1'] = 1
df_merged['ARF3/VWC2'] = 0
df_merged.loc[((df_merged['ARF3'] >= threshold) & (df_merged['Diff. (VWC2)'] > 0)) | ((df_merged['ARF3'] < threshold) & (df_merged['Diff. (VWC2)'] <= 0)), 'ARF3/VWC2'] = 1
df_merged['ARF3/VWC3'] = 0
df_merged.loc[((df_merged['ARF3'] >= threshold) & (df_merged['Diff. (VWC3)'] > 0)) | ((df_merged['ARF3'] < threshold) & (df_merged['Diff. (VWC3)'] <= 0)), 'ARF3/VWC3'] = 1

corr1 = df_merged['RF/VWC1'].sum()
corr2 = df_merged['RF/VWC2'].sum()
corr3 = df_merged['RF/VWC3'].sum()
corr4 = df_merged['ARFY/VWC1'].sum()
corr5 = df_merged['ARFY/VWC2'].sum()
corr6 = df_merged['ARFY/VWC3'].sum()
corr7 = df_merged['ARF3/VWC1'].sum()
corr8 = df_merged['ARF3/VWC2'].sum()
corr9 = df_merged['ARF3/VWC3'].sum()

print(f"Rainfall matches VWC1 with {round(corr1/730*100, 1)}%, VWC2 with {round(corr2/730*100, 1)}%, and VWC3 with {round(corr3/730*100, 1)}%")
print(f"Antecedent Rainfall (Yesterday) matches VWC1 with {round(corr4/730*100, 1)}%, VWC2 with {round(corr5/730*100, 1)}%, and VWC3 with {round(corr6/730*100, 1)}%")
print(f"Antecedent Rainfall (Three Days) matches VWC1 with {round(corr7/730*100, 1)}%, VWC2 with {round(corr8/730*100, 1)}%, and VWC3 with {round(corr9/730*100, 1)}%")

# Calculating antecedent rainfall index and checking correlation with rainy days
threshold = 16
df_merged['RF/VWC1'] = 0
df_merged.loc[((df_merged['RF'] >= threshold) & (df_merged['Diff. (VWC1)'] > 0)), 'RF/VWC1'] = 1
df_merged['RF/VWC2'] = 0
df_merged.loc[((df_merged['RF'] >= threshold) & (df_merged['Diff. (VWC2)'] > 0)), 'RF/VWC2'] = 1
df_merged['RF/VWC3'] = 0
df_merged.loc[((df_merged['RF'] >= threshold) & (df_merged['Diff. (VWC3)'] > 0)), 'RF/VWC3'] = 1
df_merged['ARFY/VWC1'] = 0
df_merged.loc[((df_merged['ARFY'] >= threshold) & (df_merged['Diff. (VWC1)'] > 0)), 'ARFY/VWC1'] = 1
df_merged['ARFY/VWC2'] = 0
df_merged.loc[((df_merged['ARFY'] >= threshold) & (df_merged['Diff. (VWC2)'] > 0)), 'ARFY/VWC2'] = 1
df_merged['ARFY/VWC3'] = 0
df_merged.loc[((df_merged['ARFY'] >= threshold) & (df_merged['Diff. (VWC3)'] > 0)), 'ARFY/VWC3'] = 1
df_merged['ARF3/VWC1'] = 0
df_merged.loc[((df_merged['ARF3'] >= threshold) & (df_merged['Diff. (VWC1)'] > 0)), 'ARF3/VWC1'] = 1
df_merged['ARF3/VWC2'] = 0
df_merged.loc[((df_merged['ARF3'] >= threshold) & (df_merged['Diff. (VWC2)'] > 0)), 'ARF3/VWC2'] = 1
df_merged['ARF3/VWC3'] = 0
df_merged.loc[((df_merged['ARF3'] >= threshold) & (df_merged['Diff. (VWC3)'] > 0)), 'ARF3/VWC3'] = 1

corr1 = df_merged['RF/VWC1'].sum()
corr2 = df_merged['RF/VWC2'].sum()
corr3 = df_merged['RF/VWC3'].sum()
corr4 = df_merged['ARFY/VWC1'].sum()
corr5 = df_merged['ARFY/VWC2'].sum()
corr6 = df_merged['ARFY/VWC3'].sum()
corr7 = df_merged['ARF3/VWC1'].sum()
corr8 = df_merged['ARF3/VWC2'].sum()
corr9 = df_merged['ARF3/VWC3'].sum()

df_merged['Rainy Day'] = 0
df_merged.loc[(df_merged['RF'] >= threshold), 'Rainy Day'] = 1
rainy_days = df_merged['Rainy Day'].sum()
df_merged['Rainy Day ARFY'] = 0
df_merged.loc[(df_merged['ARFY'] >= threshold), 'Rainy Day ARFY'] = 1
rainy_daysy = df_merged['Rainy Day ARFY'].sum()
df_merged['Rainy Day ARF3'] = 0
df_merged.loc[(df_merged['ARF3'] >= threshold), 'Rainy Day ARF3'] = 1
rainy_days3 = df_merged['Rainy Day ARF3'].sum()

print(f"\nRainfall matches VWC1 with {round(corr1/rainy_days*100, 1)}%, VWC2 with {round(corr2/rainy_days*100, 1)}%, and VWC3 with {round(corr3/rainy_days*100, 1)}%")
print(f"Antecedent Rainfall (Yesterday) matches VWC1 with {round(corr4/rainy_daysy*100, 1)}%, VWC2 with {round(corr5/rainy_daysy*100, 1)}%, and VWC3 with {round(corr6/rainy_daysy*100, 1)}%")
print(f"Antecedent Rainfall (Three Days) matches VWC1 with {round(corr7/rainy_days3*100, 1)}%, VWC2 with {round(corr8/rainy_days3*100, 1)}%, and VWC3 with {round(corr9/rainy_days3*100, 1)}%")

# Calculating the daily soil moisture drop-off
dryloss = df_merged.loc[((df_merged['RF'] == 0) & (df_merged['Diff. (VWC1)'] <= 0)), 'Diff. (VWC1)'].mean()
print(f"The average soil moisture (VWC1) loss on dry days: {round(dryloss, 4)}%")
dryloss = df_merged.loc[((df_merged['RF'] == 0) & (df_merged['Diff. (VWC2)'] <= 0)), 'Diff. (VWC2)'].mean()
print(f"The average soil moisture (VWC2) loss on dry days: {round(dryloss, 4)}%")
dryloss = df_merged.loc[((df_merged['RF'] == 0) & (df_merged['Diff. (VWC3)'] <= 0)), 'Diff. (VWC3)'].mean()
print(f"The average soil moisture (VWC3) loss on dry days: {round(dryloss, 4)}%")

dryloss = df_merged.loc[(df_merged['RF'] == 0), 'Diff. (VWC1)'].mean()
print(f"The average soil moisture (VWC1) loss on dry days: {round(dryloss, 2)}%")
dryloss = df_merged.loc[(df_merged['RF'] == 0), 'Diff. (VWC2)'].mean()
print(f"The average soil moisture (VWC2) loss on dry days: {round(dryloss, 2)}%")
dryloss = df_merged.loc[(df_merged['RF'] == 0), 'Diff. (VWC3)'].mean()
print(f"The average soil moisture (VWC3) loss on dry days: {round(dryloss, 2)}%")

dryloss = df_merged.loc[((df_merged['RF'] == 0) & (df_merged['Diff. (VWC1)'] <= 0)), 'Diff. (VWC1)'].median()
print(f"The median soil moisture (VWC1) loss on dry days: {round(dryloss, 2)}%")
dryloss = df_merged.loc[((df_merged['RF'] == 0) & (df_merged['Diff. (VWC2)'] <= 0)), 'Diff. (VWC2)'].median()
print(f"The median soil moisture (VWC2) loss on dry days: {round(dryloss, 2)}%")
dryloss = df_merged.loc[((df_merged['RF'] == 0) & (df_merged['Diff. (VWC3)'] <= 0)), 'Diff. (VWC3)'].median()
print(f"The median soil moisture (VWC3) loss on dry days: {round(dryloss, 2)}%")

dryloss = df_merged.loc[(df_merged['RF'] == 0), 'Diff. (VWC1)'].median()
print(f"The median soil moisture (VWC1) loss on dry days: {round(dryloss, 2)}%")
dryloss = df_merged.loc[(df_merged['RF'] == 0), 'Diff. (VWC2)'].median()
print(f"The median soil moisture (VWC2) loss on dry days: {round(dryloss, 2)}%")
dryloss = df_merged.loc[(df_merged['RF'] == 0), 'Diff. (VWC3)'].median()
print(f"The median soil moisture (VWC3) loss on dry days: {round(dryloss, 2)}%")

# Delete noise
del df_merged['Rainy Day']
del df_merged['Rainy Day ARFY']
del df_merged['Rainy Day ARF3']
del df_merged['RF/VWC1']
del df_merged['RF/VWC2']
del df_merged['RF/VWC3']
del df_merged['ARFY/VWC1']
del df_merged['ARFY/VWC2']
del df_merged['ARFY/VWC3']
del df_merged['ARF3/VWC1']
del df_merged['ARF3/VWC2']
del df_merged['ARF3/VWC3']

from google.colab import files
df_merged.to_csv('Daily 43 & 396.csv', encoding='utf-8-sig', index=False)
files.download('Daily 43 & 396.csv')