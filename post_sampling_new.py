import pandas as pd
import glob

import argparse

from typing import List


def read_jsonl_files(jsonl_files : List[str]):
    dfs = []
    for filename in jsonl_files:
        # Read JSONL file
        print(f"Reading {filename}")
        df = pd.read_json(filename, lines=True)
        # Add filename column
        df["source_file"] = filename
        dfs.append(df)
    return pd.concat(dfs, ignore_index=True)


def extract_target_usages(df, target_words: List[str], time_span: int):
    df['target'] = df['lemma'] + '_' + df['pos']
    df = df[df.target.isin(set(target_words))]

    # Extract year
    year = df["date"].dt.year

    # Align to 4-year bins starting at 1880
    start_year = min(year)
    df["period_start"] = ((year - start_year) // time_span) * time_span + start_year
    df["period_end"] = df["period_start"] + 3

    # Label as "YYYY-YYYY"
    df["period_label"] = df["period_start"].astype(str) + "-" + df["period_end"].astype(str)
    return df


words = {
    "adress_NN",
    "man_NN",
    "aktör_NN",
    "mask_NN",
    "broder_NN",
    "minne_NN",
    "dum_JJ",
    "moder_NN",
    "energi_NN",
    "panel_NN",
    "exportera_VB",
    "pappa_NN",
    "far_NN",
    "post_NN",
    "flicka_NN",
    "program_NN",
    "försvar_NN",
    "samtal_NN",
    "fru_NN",
    "sändning_NN",
    "herre_NN",
    "skär_JJ",
    "klimat_NN",
    "syster_NN",
    "krig_NN",
    "telefon_NN",
    "kvinna_NN",
    "väst_NN",
}


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
                    prog='Postprocess target usages files'
    )
    parser.add_argument('-t', '--target', nargs='+', default=words)
    parser.add_argument('-i', '--input-files', nargs='+', required=True)
    parser.add_argument('-o', '--output-folder', default="")
    parser.add_argument('-ts', '--time-span', default=4)
    args = parser.parse_args()

    df_all = read_jsonl_files(args.input_files)
    df_tu = extract_target_usages(df_all, args.target, args.time_span)