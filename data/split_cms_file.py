import pandas as pd

input_file = "/Users/yawboateng/github_folder/rx-cost-risk-ai/data/MUP_DPR_RY25_P04_V10_DY22_NPI.csv"
output_file = "/Users/yawboateng/github_folder/rx-cost-risk-ai/data/MUP_DPR_RY25_P04_V10_DY22_NPI_NEW.csv"

sample_rate = 0.10   # 10% of rows

print("Streaming CMS file...")
sampled_chunks = []

for chunk in pd.read_csv(input_file, chunksize=100_000):
    sampled = chunk.sample(frac=sample_rate, random_state=42)
    sampled_chunks.append(sampled)

    print(f"Processed {len(sampled_chunks)*100_000:,} rows")

sample = pd.concat(sampled_chunks)

print("Writing sampled file...")
sample.to_csv(output_file, index=False)

print("✅ cms_sample.csv created")

