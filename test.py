import pandas as pd
import json

a = pd.DataFrame({'a':[1,2],'b':[3,4]})
rename_cols = "{'a': 'x'}"
rename_cols_dict = json.loads(rename_cols.replace("'", "\""))
a.rename(index=str, columns=rename_cols_dict, inplace=True)

print(a)
