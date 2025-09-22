import argparse
import shutil
import pandas as pd

from pathlib import Path
from typing import List



def read_jsonl_files(jsonl_files : List[str]):
    dfs = []
    for filename in jsonl_files:
        print(f"Reading {filename}")
        df = pd.read_json(filename, lines=True)
        df["source_file"] = filename
        dfs.append(df)
    return pd.concat(dfs, ignore_index=True)


def extract_target_usages(df, target_words: List[str], time_span: int):
    df['target'] = df['lemma'] + '_' + df['pos']
    df = df[df.target.isin(set(target_words))]
    year = df["date"].dt.year
    start_year = min(year)
    df["period_start"] = ((year - start_year) // time_span) * time_span + start_year
    df["period_end"] = df["period_start"] + 3
    df["period_label"] = df["period_start"].astype(str) + "-" + df["period_end"].astype(str)
    return df


def get_statistics(df_tu: pd.DataFrame):
    df_tu = df_tu.groupby(['period_label', 'target']).size().reset_index(name='count')
    df_tu_pivot = df_tu.pivot(index="target", columns="period_label", values="count")
    df_tu_pivot.to_csv('timeperiod_stats.tsv', sep='\t')
    return df_tu_pivot

def write_year_target_to_json(df, out_dir : str):
    out_dir = Path(out_dir)
    if out_dir.exists() and out_dir.is_dir():
        shutil.rmtree(out_dir)
    out_dir.mkdir(parents=True, exist_ok=False)
    df.groupby(['target', 'period_label']).apply(
        lambda g: 
                g.to_json(f'{g.period_label}_{g.target}_target_usages.jsonl',
                            orient='records', lines=True, force_ascii=False),
            include_groups=False
        )


def postprocess(df_tu, 
                out_dir : str = 'postprocessing', 
                save_intermediate: bool = True, 
                min_len: int = 20,
                max_len: int = 100, 
                random_state : int  = 42
    ):
    df_filtered = df_tu[
        (df_tu['text'].str.split().str.len() > min_len) & \
        (df_tu['text'].str.split().str.len() <= max_len)
    ]
    df_stats_filtered = get_statistics(df_filtered)
    df_scrambled = df_filtered.sample(frac=1, random_state=random_state)

    if save_intermediate:
        write_year_target_to_json(df_all, out_dir= f'{out_dir}/all/')
        write_year_target_to_json(df_stats_filtered, out_dir = f'{out_dir}/filtered/')
    write_year_target_to_json(df_scrambled)


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
    postprocess(df_tu)