import pandas as pd

df = pd.read_csv('netflix_titles.csv')

# Replace the missing values with 'unknown'
df['director'] = df['director'].fillna('Unknown')
df['cast'] = df['cast'].fillna('Unknown')
df['country'] = df['country'].fillna('Unknown')
df['rating'] = df['rating'].fillna('Unknown')
df['date_added'] = df['date_added'].fillna('Not Specified')
df['duration'] = df['duration'].fillna('Unknown')

# Conversion
df['release_year'] = df['release_year'].astype(int)
df['date_added'] = pd.to_datetime(df['date_added'], errors='coerce')
df['date_added'] = df['date_added'].fillna(pd.Timestamp('1900-01-01'))

# Strip all whitespace
text_cols = ['show_id', 'type', 'title', 'director', 'cast', 'country', 'rating', 'duration', 'listed_in', 'description']
for col in text_cols:
    df[col] = df[col].astype(str).str.strip()

#Save to a cleaned csv
df.to_csv('netflix_clean.csv', index=False)