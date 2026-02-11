import pandas as pd
import psycopg2
import numpy as np

df = pd.read_csv("data/MUP_DPR_RY25_P04_V10_DY23_NPIBN.csv", dtype=str)

num_cols = [
    "Prscrbr_NPI","Tot_Clms","Tot_30day_Fills","Tot_Day_Suply","Tot_Drug_Cst","Tot_Benes",
    "GE65_Tot_Clms","GE65_Tot_30day_Fills","GE65_Tot_Drug_Cst",
    "GE65_Tot_Day_Suply","GE65_Tot_Benes"
]

for c in num_cols:
    df[c] = pd.to_numeric(df[c], errors="coerce")
    df.loc[df[c] > 1e15, c] = np.nan   # CMS overflow cleanup

df = df.replace({np.nan: None})

