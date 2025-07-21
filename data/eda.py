import pandas as pd
df = pd.read_csv('netflix_titles.csv')

nan_counts = df.isna().sum()

print(nan_counts)

rows, columns = df.shape
print(f"Total rows: {rows}")
print(f"Total columns: {columns}")

total_values = df.size  
print(f"Total values in dataset: {total_values}")

total_nan = df.isna().sum().sum()
print(f"Total missing values in dataset: {total_nan}")

duplicates = df['show_id'].duplicated()

if duplicates.any():
    print(f"Duplicate show_id found! Number of duplicates: {duplicates.sum()}")
    print(df[duplicates])
else:
    print("No duplicates found in show_id.")