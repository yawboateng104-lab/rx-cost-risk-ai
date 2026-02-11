import pandas as pd
import psycopg2
import numpy as np

# -------------------------
# RDS CONNECTION SETTINGS
# -------------------------
rds = {
    "host": "database-postgres.cbqeyi8iguo6.us-east-2.rds.amazonaws.com",
    "port": 5432,
    "user": "postgres",
    "password": "Abc123!Efg123!",
    "dbname": "rx_db"
}



print("Reading CMS CSV...")
df = pd.read_csv("/Users/yawboateng/github_folder/rx-cost-risk-ai/data/MUP_DPR_RY25_P04_V10_DY23_NPIBN.csv", dtype=str)

# -------------------------
# NUMERIC COLUMNS
# -------------------------
num_cols = [
    "Prscrbr_NPI",
    "Tot_Clms",
    "Tot_30day_Fills",
    "Tot_Day_Suply",
    "Tot_Drug_Cst",
    "Tot_Benes",
    "GE65_Tot_Clms",
    "GE65_Tot_30day_Fills",
    "GE65_Tot_Drug_Cst",
    "GE65_Tot_Day_Suply",
    "GE65_Tot_Benes"
]

# Convert CMS masked and overflow values safely
for c in num_cols:
    df[c] = pd.to_numeric(df[c], errors="coerce")
    df.loc[df[c] > 1e15, c] = np.nan   # remove CMS sentinel values

# -------------------------
# Force column order
# -------------------------
df = df[[
    "Prscrbr_NPI",
    "Prscrbr_Last_Org_Name",
    "Prscrbr_First_Name",
    "Prscrbr_City",
    "Prscrbr_State_Abrvtn",
    "Prscrbr_Type",
    "Brnd_Name",
    "Gnrc_Name",
    "Tot_Clms",
    "Tot_30day_Fills",
    "Tot_Day_Suply",
    "Tot_Drug_Cst",
    "Tot_Benes",
    "GE65_Tot_Clms",
    "GE65_Tot_30day_Fills",
    "GE65_Tot_Drug_Cst",
    "GE65_Tot_Day_Suply",
    "GE65_Tot_Benes"
]]

# Replace NaN with None for PostgreSQL
df = df.replace({np.nan: None})

# -------------------------
# PostgreSQL Insert
# -------------------------
print("Connecting to PostgreSQL...")
conn = psycopg2.connect(**rds)
cur = conn.cursor()

insert_sql = """
INSERT INTO raw.partd_2023 (
    Prscrbr_NPI,
    Prscrbr_Last_Org_Name,
    Prscrbr_First_Name,
    Prscrbr_City,
    Prscrbr_State_Abrvtn,
    Prscrbr_Type,
    Brnd_Name,
    Gnrc_Name,
    Tot_Clms,
    Tot_30day_Fills,
    Tot_Day_Suply,
    Tot_Drug_Cst,
    Tot_Benes,
    GE65_Tot_Clms,
    GE65_Tot_30day_Fills,
    GE65_Tot_Drug_Cst,
    GE65_Tot_Day_Suply,
    GE65_Tot_Benes
) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
"""

print("Loading CMS data into PostgreSQL...")

for row in df.itertuples(index=False, name=None):
    cur.execute(insert_sql, row)

conn.commit()
cur.close()
conn.close()

print("✅ CMS Part D 2023 data successfully loaded")

