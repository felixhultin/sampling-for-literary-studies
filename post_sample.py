import pandas as pd
import glob
import os

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

def read_and_filter_jsonfiles(jsonfiles: list[str]):
    raise NotImplementedError

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


SVT_TIMEPERIODS = {
    2004: 't0',
    2005: 't0',

    2006: 't1',
    2007: 't1',

    2008: 't2',
    2009: 't2',

    2010: 't3',
    2011: 't3',

    2012: 't4',
    2013: 't4',

    2014: 't5',
    2015: 't5',

    2016: 't6',
    2017: 't6',

    2018: 't7',
    2019: 't7',

    2020: 't8',
    2021: 't8',

    2022: 't9',
    2023: 't9'
}

def timeperiod_frequencies_from_svt(stats_file: str | pd.DataFrame):
    if type(stats_file) == str:
        df = pd.read_csv(stats_file)
    else:
        df = stats_file
    df['year'] = df.file.apply(lambda s: int(s[-8:-4]))
    df['t'] = df.year.apply(lambda y: SVT_TIMEPERIODS[y])
    df = df[['t', 'word', 'count']].groupby(['t', 'word']).sum('count').reset_index()
    df = df.pivot(index='word', columns='t', values='count')
    mtp_target_words = pd.read_csv('mtp_target_words.tsv', sep='\t')
    right_order = list(mtp_target_words.words)
    # Fix "backa upp"
    #right_order = ["backa upp_VB" if w == "backa_upp_VB" else w for w in right_order]
    df = df.loc[right_order]
    return df

def timeperiod_frequencies_from_kubhist(stats_file: str | pd.DataFrame):
    if type(stats_file) == str:
        df = pd.read_csv(stats_file)
    else:
        df = stats_file
    df['t'] = df.file.apply(lambda s: int(s[-12:-8]))
    df = df[['t', 'word', 'count']].groupby(['t', 'word']).sum('count').reset_index()
    df = df.pivot(index='word', columns='t', values='count')
    mtp_target_words = pd.read_csv('mtp_target_words.tsv', sep='\t')
    right_order = list(mtp_target_words.words)
    # Fix "backa upp"
    return df
    right_order = ["backa upp_VB" if w == "backa_upp_VB" else w for w in right_order]
    df = df.loc[right_order]    
    return df


def read_frequencies_from_statistic_files(statistic_files : list[str]):
    mtp_targets_file = pd.read_csv('mtp_target_words.tsv', sep='\t')
    mtp_targets = tuple(mtp_targets_file.words + '_' + mtp_targets_file.PoS)
    rows = []
    for f in statistic_files:
        print(f"Processing {f}")
        df = pd.read_csv(f, sep='\t')
        df['target'] = df['lemma'] + '_' + df['POS'].str.split('.').str[0]
        df = df[df['target'].isin(mtp_targets)]
        df = df.groupby('target').sum('count')
        target_count = df['count'].to_dict()
        for t in mtp_targets:
            row = {
                'file': f,
                'word': t.split('_')[0],
                'pos': t.split('_')[1],
                'count': target_count.get(t, 0)
            }
            rows.append(row)
    return pd.DataFrame.from_records(rows)

if __name__ == '__main__':
    #df = timeperiod_frequencies_from_svt('word_freqs.txt')
    stats_kubhist2_1880 = glob.glob(os.path.join('data/corpora/kubhist2', 'stats_kubhist2*1880*.csv.zip*'))
    stats_kubhist2_1890 = glob.glob(os.path.join('data/corpora/kubhist2', 'stats_kubhist2*1890*.csv.zip*'))
    stats_kubhist2_1900 = glob.glob(os.path.join('data/corpora/kubhist2', 'stats_kubhist2*1900*.csv.zip*'))
    stats_kubhist_1910 = glob.glob(os.path.join('data/corpora/kubhist', 'stats_kubhist*1910*.csv.zip*'))
    stats_kubhist_1920 = glob.glob(os.path.join('data/corpora/kubhist', 'stats_kubhist*1920*.csv.zip*'))
    # df = read_frequencies_from_statistic_files(
    #     stats_kubhist2_1880 + \
    #     stats_kubhist2_1890 + \
    #     stats_kubhist2_1900 + \
    #     stats_kubhist_1910 + \
    #     stats_kubhist_1920
    # )
    # df.to_csv('wordfreqs_kubhist.csv')
    # df = df[['decade', 'target', 'count']].groupby(['decade', 'target']).sum('count').reset_index()
    # t1_kubhist = glob.glob(os.path.join('t1_kubhist', '*.jsonl'))
    # t9_kubhist = glob.glob(os.path.join('t9_kubhist', '*.jsonl'))
    # t1_svt = glob.glob(os.path.join('t1_svt', '*.jsonl'))
    # t9_svt = glob.glob(os.path.join('t9_svt', '*.jsonl'))
    # df = read_frequencies_from_files(t1_kubhist + t9_kubhist + t1_svt + t9_svt)
    # df['decade'] = df['decade']\
    #     .replace({'1880': 't1_kubhist', 
    #               '1910': 't9_kubhist', 
    #               '1920': 't9_kubhist', 
    #               '2006': 't1_svt', 
    #               '2007': 't1_svt', 
    #               '2022': 't9_svt', 
    #               '2023': 't9_svt'
    #             })
    # df = df[['decade', 'target', 'count']].groupby(['decade', 'target']).sum('count').reset_index()
    # df = df.pivot(index='target', columns='decade', values='count')
    # df = df[['t1_kubhist', 't9_kubhist', 't1_svt', 't9_svt']]

    # mtp_target_words = pd.read_csv('mtp_target_words.tsv', sep='\t')
    # right_order = list(mtp_target_words.words + '_' + mtp_target_words.PoS)
    # # Fix "backa upp"
    # right_order = ["backa upp_VB" if w == "backa_upp_VB" else w for w in right_order]
    # df.loc[right_order]
    
    #df = df.groupby(['decade', 'target']).count().reset_index()
    
    # df = read_output_dir_into_df('t1_kubhist')
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