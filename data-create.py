import pandas as pd

csv_file_path = "data/hr.7257.txt"
new_csv_file_path = "data/hr.7257-time.txt"
df = pd.read_csv(csv_file_path)
timestamp_index = pd.date_range(
    start=pd.Timestamp.now(), 
    end=pd.Timestamp(2022, 12, 30, 23, 59, 59), 
    freq='0.5S') #T = minute
timestamp_col = pd.Series(timestamp_index)
df.columns =['BPM']
df["Time"] = timestamp_col
df.to_csv(new_csv_file_path, encoding='utf-8', index=False,header=False)
