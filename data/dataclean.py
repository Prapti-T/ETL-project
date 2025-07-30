import pandas as pd

# Load dataset
df = pd.read_csv('netflix_titles.csv')

# Fill missing values with standard placeholders
df['director'] = df['director'].fillna('Unknown')
df['cast'] = df['cast'].fillna('Unknown')
df['country'] = df['country'].fillna('Unknown')
df['rating'] = df['rating'].fillna('Unknown')
df['duration'] = df['duration'].fillna('Unknown')

# Convert release_year (only if you're sure there are no missing values)
df['release_year'] = df['release_year'].astype(int)

# Convert date_added to datetime and handle missing values
df['date_added'] = pd.to_datetime(df['date_added'], errors='coerce')
df['date_added'] = df['date_added'].fillna(pd.Timestamp('1900-01-01'))

# Strip whitespace from text columns
text_cols = ['show_id', 'type', 'title', 'director', 'cast', 'country', 'rating', 'duration', 'listed_in', 'description']
for col in text_cols:
    df[col] = df[col].astype(str).str.strip()

# Save cleaned data
df.to_csv('netflix_clean.csv', index=False)

print("Cleaned dataset saved as 'netflix_clean.csv'")
