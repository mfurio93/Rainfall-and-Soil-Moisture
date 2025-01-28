# Initialization
import pandas as pd
data = pd.read_csv('Hourly 43 & 396.csv')
data['RF'] = data['RF'].fillna(0)

# Step 1: Detection and exclusion of isolated rainfall measurements
data['RF-1'] = -data['RF']
for x in range(-3, 4):
  data['RF-1'] = data['RF-1'] + data['RF'].shift(x)
data['RF-1'] = round(data['RF-1'], 3)
data.loc[((data['RF'] == 0.254) & (data['RF-1'] == 0)), 'RF'] = 0
del data['RF-1']

# Step 2: Detection and exclusion of irrelevant rainfall sub-events
data['RF-2'] = 0
for x in range(-6, 7):
  data['RF-2'] = data['RF-2'] + data['RF'].shift(x)
data['RF-2'] = round(data['RF-2'], 3)
data.loc[data['RF-2'] <= 1.016, 'RF'] = 0
del data['RF-2']

# Step 3: Identification of rainfall events
consecutive_zeros = 0
for i in range(len(data)):
    if data['RF'][i] == 0:
        consecutive_zeros += 1
    else:
        if consecutive_zeros >= 48:
            data.loc[i - consecutive_zeros:i - 1, 'RF'] = -1
        consecutive_zeros = 0

# Step 4: Calculation of event rainfall, duration, and intensity
rfd = pd.DataFrame(index=range(100), columns=['Rainfall', 'Duration'])
current_sum = 0
current_count = 0
start_index = 0
for i in range(len(data)):
    if data['RF'][i] > -1:
        current_sum += data['RF'][i]
        current_count += 1
    elif current_sum > 0 :
        rfd.loc[start_index, 'Rainfall'] = current_sum
        rfd.loc[start_index, 'Duration'] = current_count
        current_sum = 0
        current_count = 0
        start_index += 1
rfd['Intensity'] = rfd['Rainfall'] / rfd['Duration']

# Rounding for presentation
rfd['Rainfall'] = pd.to_numeric(rfd['Rainfall'])
rfd['Intensity'] = pd.to_numeric(rfd['Intensity'])
rfd['Rainfall'] = rfd['Rainfall'].round(0)
rfd['Intensity'] = rfd['Intensity'].round(1)

data.to_csv('Hourly 43 & 396 (Denoised).csv', encoding='utf-8-sig', index=False)
rfd.to_csv('Rainfall Duration.csv', encoding='utf-8-sig', index=False)
