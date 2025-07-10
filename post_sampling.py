import glob
import os
import pandas as pd

from pathlib import Path


def read_frequencies_from_files(jsonl_files):
    rows = []
    for file_path in jsonl_files:
        basename = os.path.basename(file_path)
        basename_splitted = basename.split('_')
        resource_name = basename_splitted[0]
        rest_rejoined = "_".join(basename_splitted[1:])
        end_filename = 'target_usages.jsonl'
        assert file_path.endswith(end_filename)
        word_pos = rest_rejoined[:-(len(end_filename)+1)]
        splitted = resource_name.split('-')
        newspaper, decade = "-".join(splitted[1:-1]), splitted[-1]
        with open(file_path) as f:
            num_lines = sum(1 for _ in f)
        rows.append({
            'newspaper': newspaper,
            'decade': decade,
            'target': word_pos,
            'count': num_lines
        })
    return pd.DataFrame.from_records(rows)

def read_output_dir_into_df(output_dir: str):
    jsonl_files = glob.glob(os.path.join(output_dir, '*.jsonl'))

    df_list = []
    for file_path in jsonl_files:
        df = pd.read_json(file_path, lines=True)
        
        basename = os.path.basename(file_path)
        resource_name = basename.split('_')[0]
        splitted = resource_name.split('-')
        newspaper, decade = "-".join(splitted[1:-1]), splitted[-1]
        df['newspaper'] = newspaper
        df['decade'] = decade
        df_list.append(df)
    
    return pd.concat(df_list, ignore_index=True)

def filter_json_files_on_length(jsonl_files, min_len : int = 20, max_len : int = 50, output_dir = 'filtered_jsonl_files'):
    Path(output_dir).mkdir(parents=True, exist_ok=False)
    for file_path in jsonl_files:
        df = pd.read_json(file_path, lines=True)
        if len(df) < 1:
            continue
        df_filter = df[
            (df['sentence'].str.split().str.len() > min_len) & \
            (df['sentence'].str.split().str.len() <= max_len)
        ]
        basename = os.path.basename(file_path)
        print(f"Filtered {file_path} down from {len(df)} to {len(df_filter)}")
        df_filter.to_json(f"{output_dir}/{basename}", orient='records', lines=True)


def filter_sentences(
        df: pd.DataFrame, min_len : int = 20, max_len : int = 50):
    df_filter = df[
        (df['sentence'].str.split().str.len() > min_len) & \
        (df['sentence'].str.split().str.len() <= max_len)
    ]
    df_target_pos_count = df_filter\
        .groupby(['target', 'pos_tag'])\
        .size()\
        .reset_index(name='counts')
    df_target_pos_count_gt_sample_size = \
        df_target_pos_count[df_target_pos_count['counts'] >= sample_size]
    df_filter = df_filter.set_index(['target', 'pos_tag'])
    raise NotImplementedError

if __name__ == '__main__':
    t1_svt = glob.glob(os.path.join('t1_svt_twoAI', '*.jsonl'))
    t9_svt = glob.glob(os.path.join('t9_svt_twoAI', '*.jsonl'))
    df = read_frequencies_from_files(t1_svt + t9_svt)
    df['decade'] = df['decade']\
        .replace({'1880': 't1_kubhist', 
                  '1910': 't9_kubhist', 
                  '1920': 't9_kubhist', 
                  '2006': 't1_svt', 
                  '2007': 't1_svt', 
                  '2022': 't9_svt',
                  '2023': 't9_svt'
                })
    df = df[['decade', 'target', 'count']].groupby(['decade', 'target']).sum('count').reset_index()
    df = df.pivot(index='target', columns='decade', values='count')
    df = df[['t1_svt', 't9_svt']]
    mtp_target_words = pd.read_csv('mtp_target_words.tsv', sep='\t')
    right_order = list(mtp_target_words.words + '_' + mtp_target_words.PoS)
    # Fix "backa upp"
    right_order = ["backa upp_VB" if w == "backa_upp_VB" else w for w in right_order]
    df = df.loc[right_order]

    # CONTINUE HERE
    t1_svt_filtered = 'svt_t1_filtered'
    df = filter_json_files_on_length(t1_svt, min_len=15, max_len=100, output_dir = t1_svt_filtered )
    t1_svt_filtered_files = glob.glob(os.path.join(t1_svt_filtered, '*.jsonl'))
    df = read_frequencies_from_files(t1_svt_filtered_files)
    df = df[['decade', 'target', 'count']].groupby(['decade', 'target']).sum('count').reset_index()
    df = df.pivot(index='target', columns='decade', values='count')
    df = df[['t1_svt', 't9_svt']]
    mtp_target_words = pd.read_csv('mtp_target_words.tsv', sep='\t')
    right_order = list(mtp_target_words.words + '_' + mtp_target_words.PoS)
    # Fix "backa upp"
    right_order = ["backa upp_VB" if w == "backa_upp_VB" else w for w in right_order]
    df = df.loc[right_order]

    #df = read_output_dir_into_df('t1_kubhfile_pathist')
    #df_filter = select_valid_sentences(df)
    # # Get sentences with no less than 20 or more than 50 tokens
    # df_filter = df[
    #     (df['sentence'].str.split().str.len() > 20) & \
    #     (df['sentence'].str.split().str.len() <= 50)
    # ]
    # sample_size = 30
    # df_target_pos_count = df_filter\
    #     .groupby(['target', 'pos_tag'])\
    #     .size()\
    #     .reset_index(name='counts')
    # df_target_pos_count_gt_sample_size = \
    #     df_target_pos_count[df_target_pos_count['counts'] >= sample_size]
    # df_filter = df_filter.set_index(['target', 'pos_tag'])
  
    
    #df_random_sample = df_filter.groupby(['target', 'pos_tag']).sample(30)