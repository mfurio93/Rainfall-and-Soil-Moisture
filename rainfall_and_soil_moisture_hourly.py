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

# Choosing maximum rainfall, converting to inches, and cutting MM:SS
rf['RF'] = rf[['P1', 'P2']].max(axis=1)
del rf['P1']
del rf['P2']
rf['RF'] = rf['RF'] / 25.4
rf['RF'] = rf['RF'].round(2)
rf = rf.rename(columns={'fecha_hora': 'Date'})
rf['Date'] = rf['Date'].str[:-6]
rf = rf[(rf['Date'] >= '2020-03-31 00') & (rf['Date'] <= '2022-03-31 23')]

# Rounding and splitting VWC data, as well as cutting MM:SS
sm['h1'] = sm['h1'].round(1)
sm['h2'] = sm['h2'].round(1)
sm['h3'] = sm['h3'].round(1)
sm = sm.rename(columns={'h1': 'VWC1'})
sm = sm.rename(columns={'h2': 'VWC2'})
sm = sm.rename(columns={'h3': 'VWC3'})
sm = sm.rename(columns={'fecha_hora': 'Date'})
sm['Date'] = sm['Date'].str[:-6]
sm = sm[(sm['Date'] >= '2020-03-31 00') & (sm['Date'] <= '2022-03-31 23')]
sm1 = sm.copy()
sm2 = sm.copy()
sm3 = sm.copy()
sm1 = sm1.drop(columns=['VWC2', 'VWC3'])
sm2 = sm2.drop(columns=['VWC1', 'VWC3'])
sm3 = sm3.drop(columns=['VWC1', 'VWC2'])

# Count entries per day and cut hours with less than 75% entries (RF)
rf = rf.dropna(subset=['RF'])
count_rf = rf.groupby('Date')['Date'].count()
remove_rf = count_rf[count_rf < 45].index.tolist()
rf = rf[~rf['Date'].isin(remove_rf)]

# Count entries per day and cut days with less than 40% entries (VWC)
sm1 = sm1.dropna(subset=['VWC1'])
sm2 = sm2.dropna(subset=['VWC2'])
sm3 = sm3.dropna(subset=['VWC3'])
count_sm1 = sm1.groupby('Date')['Date'].count()
count_sm2 = sm2.groupby('Date')['Date'].count()
count_sm3 = sm3.groupby('Date')['Date'].count()
remove_sm1 = count_sm1[count_sm1 < 24].index.tolist()
remove_sm2 = count_sm2[count_sm2 < 24].index.tolist()
remove_sm3 = count_sm3[count_sm3 < 24].index.tolist()
sm1 = sm1[~sm1['Date'].isin(remove_sm1)]
sm2 = sm2[~sm2['Date'].isin(remove_sm2)]
sm3 = sm3[~sm3['Date'].isin(remove_sm3)]

# Sum rainfall data to get total daily rainfall
rf_grouped = rf.groupby('Date').agg({'RF': 'sum'})
rf_grouped['RF'] = rf_grouped['RF'].round(2)

# Average soil moisture data to get average soil moisture
sm1_grouped = sm1.groupby('Date').agg({'VWC1': 'mean'})
sm2_grouped = sm2.groupby('Date').agg({'VWC2': 'mean'})
sm3_grouped = sm3.groupby('Date').agg({'VWC3': 'mean'})
sm1_grouped['VWC1'] = sm1_grouped['VWC1'].round(1)
sm2_grouped['VWC2'] = sm2_grouped['VWC2'].round(1)
sm3_grouped['VWC3'] = sm3_grouped['VWC3'].round(1)

# Create a dataframe for every hour
ranges = pd.date_range(start='03/31/2020 00', end='03/31/2022 23', freq='h')
dates = pd.DataFrame(ranges, columns=['Date'])
dates['Date'] = dates['Date'].dt.strftime('%Y-%m-%d %H')

# Merge all files
df_merged = pd.merge(dates, rf_grouped, on='Date', how='left')
df_merged = pd.merge(df_merged, sm1_grouped, on='Date', how='left')
df_merged = pd.merge(df_merged, sm2_grouped, on='Date', how='left')
df_merged = pd.merge(df_merged, sm3_grouped, on='Date', how='left')
df_merged['RF'] = round(df_merged['RF'] * 25.4, 3)

# Calculating VWC differences and RF offsets
df_merged['Diff. (VWC1)'] = round(df_merged['VWC1'].diff(), 1)
df_merged['Diff. (VWC2)'] = round(df_merged['VWC2'].diff(), 1)
df_merged['Diff. (VWC3)'] = round(df_merged['VWC3'].diff(), 1)
for x in range(1, 25):
  df_merged[f'RF-{x}'] = df_merged['RF'].shift(x)
df_merged = df_merged[df_merged['Date'] >= '2020-04-01 00']

# Calculating offset rainfall and checking correlation
df_merged['RF/VWC1'] = 0
df_merged.loc[((df_merged['RF'] >= 1) & (df_merged['Diff. (VWC1)'] > 0)) | ((df_merged['RF'] < 1) & (df_merged['Diff. (VWC1)'] <= 0)), 'RF/VWC1'] = 1
df_merged['RF/VWC2'] = 0
df_merged.loc[((df_merged['RF'] >= 1) & (df_merged['Diff. (VWC2)'] > 0)) | ((df_merged['RF'] < 1) & (df_merged['Diff. (VWC2)'] <= 0)), 'RF/VWC2'] = 1
df_merged['RF/VWC3'] = 0
df_merged.loc[((df_merged['RF'] >= 1) & (df_merged['Diff. (VWC3)'] > 0)) | ((df_merged['RF'] < 1) & (df_merged['Diff. (VWC3)'] <= 0)), 'RF/VWC3'] = 1

corr1 = df_merged['RF/VWC1'].sum()
corr2 = df_merged['RF/VWC2'].sum()
corr3 = df_merged['RF/VWC3'].sum()

for x in range(1, 25):
  df_merged[f'RF-{x}/VWC1'] = 0
  df_merged.loc[((df_merged[f'RF-{x}'] >= 1) & (df_merged['Diff. (VWC1)'] > 0)) | ((df_merged[f'RF-{x}'] < 1) & (df_merged['Diff. (VWC1)'] <= 0)), f'RF-{x}/VWC1'] = 1
  df_merged[f'RF-{x}/VWC2'] = 0
  df_merged.loc[((df_merged[f'RF-{x}'] >= 1) & (df_merged['Diff. (VWC2)'] > 0)) | ((df_merged[f'RF-{x}'] < 1) & (df_merged['Diff. (VWC2)'] <= 0)), f'RF-{x}/VWC2'] = 1
  df_merged[f'RF-{x}/VWC3'] = 0
  df_merged.loc[((df_merged[f'RF-{x}'] >= 1) & (df_merged['Diff. (VWC3)'] > 0)) | ((df_merged[f'RF-{x}'] < 1) & (df_merged['Diff. (VWC3)'] <= 0)), f'RF-{x}/VWC3'] = 1

corr4 = np.zeros(25)
corr5 = np.zeros(25)
corr6 = np.zeros(25)

for x in range(1,25):
  corr4[x] = df_merged[f'RF-{x}/VWC1'].sum()
  corr5[x] = df_merged[f'RF-{x}/VWC2'].sum()
  corr6[x] = df_merged[f'RF-{x}/VWC3'].sum()

print(f"Rainfall matches VWC1 with {round(corr1/17520*100, 1)}%, VWC2 with {round(corr2/17520*100, 1)}%, and VWC3 with {round(corr3/17520*100, 1)}%")
for x in range (1,25):
  print(f"Offset Rainfall (RF-{x}) matches VWC1 with {round(corr4[x]/17520*100, 1)}%, VWC2 with {round(corr5[x]/17520*100, 1)}%, and VWC3 with {round(corr6[x]/17520*100, 1)}%")

# Calculating offset rainfall and checking correlation with rainy hours
threshold = 10
df_merged['RF/VWC1'] = 0
df_merged.loc[((df_merged['RF'] >= threshold) & (df_merged['Diff. (VWC1)'] > 0)), 'RF/VWC1'] = 1
df_merged['RF/VWC2'] = 0
df_merged.loc[((df_merged['RF'] >= threshold) & (df_merged['Diff. (VWC2)'] > 0)), 'RF/VWC2'] = 1
df_merged['RF/VWC3'] = 0
df_merged.loc[((df_merged['RF'] >= threshold) & (df_merged['Diff. (VWC3)'] > 0)), 'RF/VWC3'] = 1

corr1 = df_merged['RF/VWC1'].sum()
corr2 = df_merged['RF/VWC2'].sum()
corr3 = df_merged['RF/VWC3'].sum()

for x in range(1, 25):
  df_merged[f'RF-{x}/VWC1'] = 0
  df_merged.loc[((df_merged[f'RF-{x}'] >= threshold) & (df_merged['Diff. (VWC1)'] > 0)), f'RF-{x}/VWC1'] = 1
  df_merged[f'RF-{x}/VWC2'] = 0
  df_merged.loc[((df_merged[f'RF-{x}'] >= threshold) & (df_merged['Diff. (VWC2)'] > 0)), f'RF-{x}/VWC2'] = 1
  df_merged[f'RF-{x}/VWC3'] = 0
  df_merged.loc[((df_merged[f'RF-{x}'] >= threshold) & (df_merged['Diff. (VWC3)'] > 0)), f'RF-{x}/VWC3'] = 1

corr4 = np.zeros(25)
corr5 = np.zeros(25)
corr6 = np.zeros(25)

for x in range(1,25):
  corr4[x] = df_merged[f'RF-{x}/VWC1'].sum()
  corr5[x] = df_merged[f'RF-{x}/VWC2'].sum()
  corr6[x] = df_merged[f'RF-{x}/VWC3'].sum()

df_merged['Rainy Hour'] = 0
df_merged.loc[(df_merged['RF'] >= threshold), 'Rainy Hour'] = 1
rainy_hours = df_merged['Rainy Hour'].sum()

print(f"Rainfall matches VWC1 with {round(corr1/rainy_hours*100, 1)}%, VWC2 with {round(corr2/rainy_hours*100, 1)}%, and VWC3 with {round(corr3/rainy_hours*100, 1)}%")
for x in range (1,25):
  print(f"Offset Rainfall (RF-{x}) matches VWC1 with {round(corr4[x]/rainy_hours*100, 1)}%, VWC2 with {round(corr5[x]/rainy_hours*100, 1)}%, and VWC3 with {round(corr6[x]/rainy_hours*100, 1)}%")

# Calculating the minutely soil moisture drop-off
dryloss = df_merged.loc[((df_merged['RF'] == 0) & (df_merged['Diff. (VWC1)'] <= 0)), 'Diff. (VWC1)'].mean()
print(f"The average soil moisture (VWC1) loss on dry hours: {round(dryloss, 4)}%")
dryloss = df_merged.loc[((df_merged['RF'] == 0) & (df_merged['Diff. (VWC2)'] <= 0)), 'Diff. (VWC2)'].mean()
print(f"The average soil moisture (VWC2) loss on dry hours: {round(dryloss, 4)}%")
dryloss = df_merged.loc[((df_merged['RF'] == 0) & (df_merged['Diff. (VWC3)'] <= 0)), 'Diff. (VWC3)'].mean()
print(f"The average soil moisture (VWC3) loss on dry hours: {round(dryloss, 4)}%")

dryloss = df_merged.loc[(df_merged['RF'] == 0), 'Diff. (VWC1)'].mean()
print(f"The average soil moisture (VWC1) loss on dry hours: {round(dryloss, 4)}%")
dryloss = df_merged.loc[(df_merged['RF'] == 0), 'Diff. (VWC2)'].mean()
print(f"The average soil moisture (VWC2) loss on dry hours: {round(dryloss, 4)}%")
dryloss = df_merged.loc[(df_merged['RF'] == 0), 'Diff. (VWC3)'].mean()
print(f"The average soil moisture (VWC3) loss on dry hours: {round(dryloss, 4)}%")

dryloss = df_merged.loc[((df_merged['RF'] == 0) & (df_merged['Diff. (VWC1)'] <= 0)), 'Diff. (VWC1)'].median()
print(f"The median soil moisture (VWC1) loss on dry hours: {round(dryloss, 4)}%")
dryloss = df_merged.loc[((df_merged['RF'] == 0) & (df_merged['Diff. (VWC2)'] <= 0)), 'Diff. (VWC2)'].median()
print(f"The median soil moisture (VWC2) loss on dry hours: {round(dryloss, 4)}%")
dryloss = df_merged.loc[((df_merged['RF'] == 0) & (df_merged['Diff. (VWC3)'] <= 0)), 'Diff. (VWC3)'].median()
print(f"The median soil moisture (VWC3) loss on dry hours: {round(dryloss, 4)}%")

dryloss = df_merged.loc[(df_merged['RF'] == 0), 'Diff. (VWC1)'].median()
print(f"The median soil moisture (VWC1) loss on dry hours: {round(dryloss, 4)}%")
dryloss = df_merged.loc[(df_merged['RF'] == 0), 'Diff. (VWC2)'].median()
print(f"The median soil moisture (VWC2) loss on dry hours: {round(dryloss, 4)}%")
dryloss = df_merged.loc[(df_merged['RF'] == 0), 'Diff. (VWC3)'].median()
print(f"The median soil moisture (VWC3) loss on dry hours: {round(dryloss, 4)}%")

# Delete noise
del df_merged['Rainy Hour']
del df_merged['RF/VWC1']
del df_merged['RF/VWC2']
del df_merged['RF/VWC3']
#for x in range(6, 25):
  #del df_merged[f'RF-{x}']
  #del df_merged[f'RF-{x}/VWC1']
  #del df_merged[f'RF-{x}/VWC2']
  #del df_merged[f'RF-{x}/VWC3']

from google.colab import files
df_merged.to_csv('Hourly 43 & 396.csv', encoding='utf-8-sig', index=False)
files.download('Hourly 43 & 396.csv')