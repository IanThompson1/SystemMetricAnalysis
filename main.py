import pandas as pd

# Load data
df = pd.read_csv('data.csv')

# Display first few rows
print(df.head())

# Get basic information
print(df.info())
print(df.describe())

# Filter data
filtered_df = df[df['column_name'] > 0]

# Group and aggregate
grouped = df.groupby('category').sum()

# Save results
df.to_csv('output.csv', index=False)