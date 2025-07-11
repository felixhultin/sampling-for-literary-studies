import glob
import os
import pandas as pd
import shutil

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
        df = pd.read_json(file_path, lines=True, dtype= {"date": str})
        
        basename = os.path.basename(file_path)
        resource_name = basename.split('_')[0]
        splitted = resource_name.split('-')
        newspaper, decade = "-".join(splitted[1:-1]), splitted[-1]
        df['newspaper'] = newspaper
        df['decade'] = decade
        df_list.append(df)
    
    return pd.concat(df_list, ignore_index=True)

def filter_json_files_on_length(jsonl_files, min_len : int = 20, max_len : int = 100, output_dir = 'filtered_jsonl_files'):
    out_dir = Path(output_dir)
    if out_dir.exists() and out_dir.is_dir():
        shutil.rmtree(out_dir)
    out_dir.mkdir(parents=True, exist_ok=False)
    for file_path in jsonl_files:
        df = pd.read_json(file_path, lines=True, dtype= {"date": str})
        if len(df) < 1:
            continue
        df_filter = df[
            (df['sentence'].str.split().str.len() > min_len) & \
            (df['sentence'].str.split().str.len() <= max_len)
        ]
        basename = os.path.basename(file_path)
        df_filter.to_json(f"{output_dir}/{basename}", orient='records', lines=True, force_ascii=False)

def combine_json_files_on_word(jsonl_files, output_dir = 'combined'):
    output_files = {}
    for file_path in jsonl_files:
        basename = os.path.basename(file_path)
        splitted = basename.split('_')
        rest = "_".join(splitted[1:])
        value = output_files.setdefault(rest, [])
        value.append(file_path)
    out_dir = Path(output_dir)
    if out_dir.exists() and out_dir.is_dir():
        shutil.rmtree(out_dir)
    out_dir.mkdir(parents=True, exist_ok=False) 
    for w, file_paths in output_files.items():
        to_combine = []
        for fp in file_paths:
            basename = os.path.basename(fp)
            resource = basename.split('_')[0]
            df = pd.read_json(fp, lines=True, dtype= {"date": str})
            df['resource'] = resource
            to_combine.append(df)
        combined = pd.concat(to_combine)
        combined.to_json(f"{output_dir}/{w}", orient='records', lines=True, force_ascii=False)


def random_sample_json_files(jsonl_files, output_dir = 'random_sample', min_sample_size = 20, max_extra_samples = 20):
    out_dir = Path(output_dir)
    if out_dir.exists() and out_dir.is_dir():
        shutil.rmtree(out_dir)
    out_dir.mkdir(parents=True, exist_ok=False)
    for file_path in jsonl_files:
        basename = os.path.basename(file_path)
        df = pd.read_json(file_path, lines=True, dtype= {"date": str})
        if len(df) < min_sample_size:
            continue
        else:
            if len(df) > min_sample_size + max_extra_samples:
                random_sample = df.sample(min_sample_size + max_extra_samples)
            else:
                random_sample = df.sample(len(df))
        random_sample.to_json(f"{output_dir}/{basename}", orient='records', lines=True, force_ascii=False)

def remove_files_not_in_words2keep(jsonl_files, words2keep : list[str], output_dir = 'wordsremoved'):
    out_dir = Path(output_dir)
    if out_dir.exists() and out_dir.is_dir():
        shutil.rmtree(out_dir)
    out_dir.mkdir(parents=True, exist_ok=False)
    for file_path in jsonl_files:
        basename = os.path.basename(file_path)
        word = basename.split('_')[0]
        if word in words2keep:
            df = pd.read_json(file_path, lines=True, dtype= {"date": str})
            df.to_json(f"{output_dir}/{basename}", orient='records', lines=True, force_ascii=False)

def clean_up_words(df, t1_svt, t9_svt, t1_kubhist, t9_kubhist):
    for _, row in df.iterrows():
        word = row['word']
        to_keep = True
        if 'SVT' and 'Kubhist' in row.corpora:
            for tp in (t1_svt, t9_svt, t1_kubhist, t9_kubhist):
                if not glob.glob(f"{tp}/{word}*"):
                    to_keep = False
        elif 'SVT' in row.corpora:
            for tp in (t1_svt, t9_svt):
                if not glob.glob(f"{tp}/{word}*"):
                    to_keep = False
        elif 'Kubhist' in row.corpora:
            for tp in (t1_kubhist, t9_kubhist):
                if not glob.glob(f"{tp}/{word}*"):
                    to_keep = False
        elif 'flashback' in row.corpora or 'Flashback' in row.corpora or 'parliamentary data' in row.corpora:
            to_keep = False
        if not to_keep:
            for tp in (t1_svt, t9_svt, t1_kubhist, t9_kubhist):
                for f in glob.glob(f"{tp}/{word}*"):
                    os.remove(f)

def post_sample(input_dir: str, words2keep):
    input_dir_files = glob.glob(os.path.join(input_dir, '*.jsonl'))
    output_dir_filtered = input_dir + '_filtered'
    filter_json_files_on_length(input_dir_files, min_len=15, max_len=100, output_dir = output_dir_filtered )
    
    output_dir_filtered_files = glob.glob(os.path.join(output_dir_filtered, '*.jsonl'))
    output_dir_combined = input_dir + '_combined'
    combine_json_files_on_word(output_dir_filtered_files, output_dir_combined)

    output_dir_combined_files = glob.glob(os.path.join(output_dir_combined, '*.jsonl'))
    output_dir_random_samples = input_dir + '_random_samples'
    random_sample_json_files(output_dir_combined_files, output_dir=output_dir_random_samples)

    output_dir_random_samples_files = glob.glob(os.path.join(output_dir_random_samples, '*.jsonl'))
    output_dir_words2keep_output_dir = input_dir + '_random_samples_final'
    remove_files_not_in_words2keep(output_dir_random_samples_files, words2keep, output_dir_words2keep_output_dir)


if __name__ == '__main__':
    words2keep = pd.read_csv('words2keep.csv').word.tolist()
    t1_svt_dir, t9_svt_dir, t1_kubhist_dir, t9_kubhist_dir = 't1_svt', 't9_svt', 't1_kubhist', 't9_kubhist'
    post_sample(t1_svt_dir, words2keep)
    post_sample(t9_svt_dir, words2keep)
    post_sample(t1_kubhist_dir, words2keep)
    post_sample(t9_kubhist_dir, words2keep)

    clean_up_words(
        pd.read_csv('words2keep.csv'),
        t1_svt_dir + '_random_samples_final',
        t9_svt_dir + '_random_samples_final',
        t1_kubhist_dir + '_random_samples_final',
        t9_kubhist_dir + '_random_samples_final'
    )