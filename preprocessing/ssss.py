import dask
import pandas as pd
import dask.dataframe as dd
import numpy as np

print(dask.config.get("dataframe.convert-string"))

dask.config.set({"dataframe.convert-string": False})

print(dask.config.get("dataframe.convert-string"))

df = dd.from_pandas(pd.DataFrame({'A': [[0, 1, 2], 'foo', [], [3, 4]],
                                  'B': 1,
                                  'C': [['a', 'b', 'c'], np.nan, [], ['d', 'e']]}), npartitions=2)

print(df.compute())
print(df.explode('A').compute())


# Mock dane z API
# Mock dane z API
df_api = pd.DataFrame({
    'ELI': ['A', 'B', 'C'],
    'ELI_annotation': ['A1', 'B1', 'C1'],
    'type_annotation': ['typeA', 'typeB', 'typeC']
})

# Mock dane z DB
df_db = pd.DataFrame({
    'ELI': ['A', 'B', 'D'],
    'ELI_annotation': ['A1', 'B1', 'D1'],
    'type_annotation': ['typeA_db', 'typeB_db', 'typeD_db']
})

# Meta do ddf_db — bez kolumny _merge
meta_db = pd.DataFrame({
    'ELI': pd.Series(dtype='str'),
    'ELI_annotation': pd.Series(dtype='str'),
    'type_annotation': pd.Series(dtype='str')
})

# Konwersja do Dask
ddf_api = dd.from_pandas(df_api, npartitions=1)
ddf_db = dd.from_pandas(df_db, npartitions=1)


# Merge bez indicator (bo nie działa niezawodnie w Dask)
merged = ddf_api.merge(
    ddf_db,
    on=['ELI', 'ELI_annotation'],
    how='outer',
    suffixes=('_api', '_db'),
    indicator=True
).compute()

r = 4