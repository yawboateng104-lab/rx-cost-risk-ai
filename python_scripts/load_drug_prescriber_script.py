import pandas as pd
import psycopg2

df = pd.read_csv("/Users/yawboateng/github_folder/rx-cost-risk-ai/data/MUP_DPR_RY25_P04_V10_DY23_NPIBN.csv")

conn = psycopg2.connect(**rds_config)
cur = conn.cursor()

for _, row in df.iterrows():
    cur.execute("""
    INSERT INTO raw.part_d_prescribers (
        npi,
        nppes_provider_last_org_name,
        generic_name,
        total_drug_cost,
        total_claim_count,
        bene_count
    ) VALUES (%s, %s, %s, %s, %s, %s)
    ON CONFLICT DO NOTHING
    """, (
        row['npi'],
        row['nppes_provider_last_org_name'],
        row['generic_name'],
        row['total_drug_cost'],
        row['total_claim_count'],
        row['bene_count']
    ))

conn.commit()
cur.close()
conn.close()

