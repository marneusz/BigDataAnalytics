import pandas as pd
import numpy as np

df = pd.read_csv("happy_csv.csv", index_col = 0, sep = ',')
df.replace({'\r': '<br>'}, regex=True, inplace=True)
df.replace({'\n': '<br>'}, regex=True, inplace=True)

with open("RS_2014-10.csv", mode='w', newline='\n') as f:
	df.to_csv(f, sep=",", float_format='%.2f',index=False)



