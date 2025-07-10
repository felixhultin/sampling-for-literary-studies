import pandas as pd
import glob
import os

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

def timeperiod_frequencies_from_svt(stats_file: str | pd.DataFrame, 
                                    mtp_target_words: pd.DataFrame ):
    if type(stats_file) == str:
        df = pd.read_csv(stats_file)
    else:
        df = stats_file
    df['year'] = df.file.apply(lambda s: int(s[-8:-4]))
    df['t'] = df.year.apply(lambda y: SVT_TIMEPERIODS[y])
    df = df[['t', 'word', 'count']].groupby(['t', 'word']).sum('count').reset_index()
    df = df.pivot(index='word', columns='t', values='count')
    mtp_target_words['words'] = mtp_target_words['words'].replace('backa_upp', 'backa upp')
    right_order = list(mtp_target_words.words)
    df = df.loc[right_order]
    return df

def timeperiod_frequencies_from_kubhist(stats_file: str | pd.DataFrame, 
                                        mtp_target_words: pd.DataFrame):
    if type(stats_file) == str:
        df = pd.read_csv(stats_file)
    else:
        df = stats_file
    df['t'] = df.file.apply(lambda s: int(s[-12:-8]))
    df = df[['t', 'word', 'count']].groupby(['t', 'word']).sum('count').reset_index()
    df = df.pivot(index='word', columns='t', values='count')
    mtp_target_words['words'] = mtp_target_words['words'].replace('backa_upp', 'backa upp')
    right_order = list(mtp_target_words.words)
    df = df.loc[right_order]
    return df

def choose_samples_by_frequency(df : pd.DataFrame, frequency = 20):
    to_keep = []
    for index, row in df.iterrows():
        keep = True
        if 'SVT'in row.corpora:
            for tp in ('t' + str(n) for n in range(1,10)):
                if row[tp] < frequency:
                    keep = False
        if 'Kubhist' in row.corpora:
            for tp in (1880, 1890, 1900, 1910, 1920):
                if row[tp] < frequency:
                    keep = False
        if keep:    
            to_keep.append({'word': index} | row.to_dict())
    return pd.DataFrame.from_records(to_keep).drop(columns='t0')

if __name__ == '__main__':
    mtp_target_words = pd.read_csv('mtp_target_words.tsv', sep='\t')
    stats_kubhist2_1880 = glob.glob(os.path.join('data/corpora/kubhist2', 'stats_kubhist2*1880*.csv.zip*'))
    stats_kubhist2_1890 = glob.glob(os.path.join('data/corpora/kubhist2', 'stats_kubhist2*1890*.csv.zip*'))
    stats_kubhist2_1900 = glob.glob(os.path.join('data/corpora/kubhist2', 'stats_kubhist2*1900*.csv.zip*'))
    stats_kubhist_1910 = glob.glob(os.path.join('data/corpora/kubhist', 'stats_kubhist*1910*.csv.zip*'))
    stats_kubhist_1920 = glob.glob(os.path.join('data/corpora/kubhist', 'stats_kubhist*1920*.csv.zip*'))
    kubhist_out = 'wordfreqs_kubhist.csv'
    df_kubhist = read_frequencies_from_statistic_files(
        stats_kubhist2_1880 + \
        stats_kubhist2_1890 + \
        stats_kubhist2_1900 + \
        stats_kubhist_1910 + \
        stats_kubhist_1920
    ).to_csv(kubhist_out)
    df_kubhist_overview = timeperiod_frequencies_from_kubhist(kubhist_out, mtp_target_words)
    df_kubhist_overview.to_csv('wordfreqs_kubhist-overview.csv')
    svt_out = 'wordfreqs_svt.csv'
    stats_svt = glob.glob(os.path.join('data/corpora/svt', 'stats_*.csv'))
    df_svt = read_frequencies_from_statistic_files(stats_svt).to_csv(svt_out)
    df_svt_overview = timeperiod_frequencies_from_svt(svt_out, mtp_target_words)
    df_svt_overview.to_csv('wordfreqs_svt-overview.csv')
    df_all_overview = df_kubhist_overview.join(df_svt_overview)
    df_all_overview.to_csv('wordfreqs_all-overview.csv')

    # Words to sample from based on frequencies
    df_all_overview = df_all_overview.drop(index='AI')
    mtp_target_words = mtp_target_words.set_index('words').drop(index='AI')
    df_all_overview['corpora'] = mtp_target_words['dataset/s']


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