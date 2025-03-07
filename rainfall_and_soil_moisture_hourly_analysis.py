# Initialization
import numpy as np
import pandas as pd
data = pd.read_csv('Hourly 43 & 396.csv')

# Cutting rainfall offsets above 5 hours
for x in range(6, 25):
  del data[f'RF-{x}']

# Marking rows in range
t1 = 0
t2 = 0

data['X-0'] = 0
data.loc[((data['RF'] >= t1) & (data['RF'] <= t2)), 'X-0'] = 1

for x in range(1, 6):
  data[f'X-{x}'] = 0
  data.loc[((data[f'RF-{x}'] >= t1) & (data[f'RF-{x}'] <= t2)), f'X-{x}'] = 1

for x in range (6):
  print(f"{round(data.loc[data[f'X-{x}'] == 1, 'VWC1'].max(), 1)},{round(data.loc[data[f'X-{x}'] == 1, 'VWC1'].mean(), 1)},{round(data.loc[data[f'X-{x}'] == 1, 'VWC1'].median(), 1)}")
  print(f"{round(data.loc[data[f'X-{x}'] == 1, 'VWC2'].max(), 1)},{round(data.loc[data[f'X-{x}'] == 1, 'VWC2'].mean(), 1)},{round(data.loc[data[f'X-{x}'] == 1, 'VWC2'].median(), 1)}")
  print(f"{round(data.loc[data[f'X-{x}'] == 1, 'VWC3'].max(), 1)},{round(data.loc[data[f'X-{x}'] == 1, 'VWC3'].mean(), 1)},{round(data.loc[data[f'X-{x}'] == 1, 'VWC3'].median(), 1)}")
  print(f"{round(data.loc[data[f'X-{x}'] == 1, 'Diff. (VWC1)'].max(), 2)},{round(data.loc[data[f'X-{x}'] == 1, 'Diff. (VWC1)'].mean(), 2)},{round(data.loc[data[f'X-{x}'] == 1, 'Diff. (VWC1)'].median(), 2)}")
  print(f"{round(data.loc[data[f'X-{x}'] == 1, 'Diff. (VWC2)'].max(), 2)},{round(data.loc[data[f'X-{x}'] == 1, 'Diff. (VWC2)'].mean(), 2)},{round(data.loc[data[f'X-{x}'] == 1, 'Diff. (VWC2)'].median(), 2)}")
  print(f"{round(data.loc[data[f'X-{x}'] == 1, 'Diff. (VWC3)'].max(), 2)},{round(data.loc[data[f'X-{x}'] == 1, 'Diff. (VWC3)'].mean(), 2)},{round(data.loc[data[f'X-{x}'] == 1, 'Diff. (VWC3)'].median(), 2)}\n\n")

from google.colab import files
data.to_csv('Test.csv', encoding='utf-8-sig', index=False)