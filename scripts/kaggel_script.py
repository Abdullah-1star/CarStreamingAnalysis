import pandas as pd

input_file = "/kaggle/input/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store/2019-Nov.csv"

# Read the dataset in chunks
chunks = []

for chunk in pd.read_csv(input_file, chunksize=100_000):
    chunks.append(chunk)

df = pd.concat(chunks, ignore_index=True)

# Randomly select 50,000 rows
sample_50k = df.sample(n=100_000, random_state=42)

# Save the result
output_file = "/kaggle/working/100k.csv"
sample_50k.to_csv(output_file, index=False)

print("Done!")
print("Rows:", len(sample_50k))
print("File:", output_file)