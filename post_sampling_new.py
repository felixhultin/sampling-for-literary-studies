import pandas as pd
import glob

words = {
    "adress",
    "man",
    "aktör",
    "mask",
    "broder",
    "minne",
    "dum",
    "moder",
    "energi",
    "panel",
    "exportera",
    "pappa",
    "far",
    "post",
    "flicka",
    "program",
    "försvar",
    "samtal",
    "fru",
    "sändning",
    "herre",
    "skär",
    "klimat",
    "syster",
    "krig",
    "telefon",
    "kvinna",
    "väst",
}


# Path to your JSONL files
path = "../kubhist2/*.jsonl"

# Collect DataFrames
dfs = []
for filename in glob.glob(path):
    # Read JSONL file
    print(f"Reading {filename}")
    df = pd.read_json(filename, lines=True)
    # Add filename column
    df["source_file"] = filename
    dfs.append(df)

# Combine all into one DataFrame
df = pd.concat(dfs, ignore_index=True)
df = df[df.lemma.isin(words)]

# Extract year
year = df["date"].dt.year

# Align to 4-year bins starting at 1880
start_year = 1880
df["period_start"] = ((year - start_year) // 4) * 4 + start_year
df["period_end"] = df["period_start"] + 3

# Label as "YYYY-YYYY"
df["period_label"] = df["period_start"].astype(str) + "-" + df["period_end"].astype(str)