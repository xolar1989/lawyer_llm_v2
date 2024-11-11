import pandas as pd

# Example of stored data
df_stored = pd.DataFrame({
    'ID': [1, 2, 3,4],
    'Name': ['Alice', 'Bob', 'Charlie','David'],
    'Age': [25, 30, 35,40],
    'City': ['New York', 'Los Angeles', 'Chicago','San Francisco']
})

# Example of new data with potential changes
df_new = pd.DataFrame({
    'ID': [1, 2, 3],
    'Name': ['Alice', 'Bob', 'Charles'],  # Charlie has been changed to Charles
    'Age': [25, 31, 35],  # Bob's age has been updated
    'City': ['New York', 'Los Angeles', 'San Francisco']  # Charlie has moved
})

# Merge on the ID column
df_combined = pd.merge(df_stored, df_new, on='ID', suffixes=('_stored', '_new'))

# Identify where the properties (columns) differ
for column in ['Name', 'Age', 'City']:
    df_combined[f'{column}_changed'] = df_combined[f'{column}_stored'] != df_combined[f'{column}_new']

# Display the rows with changes
changes = df_combined[df_combined[['Name_changed', 'Age_changed', 'City_changed']].any(axis=1)]
print(changes)
