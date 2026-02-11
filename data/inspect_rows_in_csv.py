cd /Users/yawboateng/github_folder/rx-cost-risk-ai/data
python3 - <<EOF
import pandas as pd
df = pd.read_csv("MUP_DPR_RY25_P04_V10_DY23_NPIBN.csv", nrows=5)
print(df.columns.tolist())
print(df.head())
EOF




###use this on command line

python3 -c "import pandas as pd; df=pd.read_csv('data/MUP_DPR_RY25_P04_V10_DY23_NPIBN.csv', nrows=5); print(df.columns.tolist()); print(df.head())"
