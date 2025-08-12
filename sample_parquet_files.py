import argparse
import datetime
import pandas as pd

import pyarrow.parquet as pq


def extract_target_usages(
        parquet_filepath : str, 
        targets : set[str], 
        start_date: str, 
        end_date: str,
        chunksize : int = 10 ** 6,
        write2json : bool = True
    ):
    start_timestamp = pd.Timestamp(start_date).date()
    end_timestamp = pd.Timestamp(end_date).date()
    parquet_file = pq.ParquetFile(parquet_filepath)
    target_words, _ = zip(*[t.split('_') for t in targets])
    expression = '|'.join(target_words)
    occurences, sentence_tokens = [], []
    start_index = 0
    for i in parquet_file.iter_batches(batch_size=chunksize):
        chunk = i.to_pandas()
        if chunk.iloc[0].date >= end_timestamp:
            break
        elif chunk.iloc[0].date <= start_timestamp and chunk.iloc[-1].date <= end_timestamp:
            pass
        else:
            chunk.index = range(start_index, start_index + len(chunk))
            start_index += len(chunk)
            chunk_occurences = chunk[
                (chunk['lemma'].str.contains(expression))
            ]
            chunk_occurences.loc[:, 'lemma'] = chunk_occurences.lemma.str.split('|')
            chunk_occurences = chunk_occurences.explode('lemma')
            chunk_occurences['target'] = chunk_occurences['lemma'] + '_' + chunk_occurences['pos']
            chunk_occurences = chunk_occurences[chunk_occurences['target'].isin(targets)]
            chunk_occurences = chunk_occurences[
                (chunk_occurences['date'] >= start_timestamp) & (chunk_occurences['date'] <= end_timestamp) 
            ]
            sentence_ids = chunk_occurences.sentence_id.unique()
            chunk_sentences = chunk[chunk['sentence_id'].isin(sentence_ids)]
            occurences.append(chunk_occurences)
            sentence_tokens.append(chunk_sentences)
            
        print(f"{start_index} lines read.")
    
    df_targets = pd.concat(occurences)
    df_tokens = pd.concat(sentence_tokens)

    df_tokens['length'] = df_tokens['token'].str.len()
    df_tokens['space'] = 1  # space after token
    is_last = df_tokens['sentence_id'] != df_tokens['sentence_id'].shift(-1)
    df_tokens.loc[is_last, 'space'] = 0

    df_tokens['start'] = \
        df_tokens.groupby('sentence_id')[['length', 'space']].cumsum()['length'] - \
        df_tokens['length'] + df_tokens.groupby('sentence_id')['space'].cumsum() - \
        df_tokens['space']

    df_tokens['end'] = df_tokens['start'] + df_tokens['length']
    df_tokens = df_tokens.drop(columns=['length', 'space'])
    df_tokens = df_tokens.astype({'start': 'Int16', 'end': 'Int16'})

    df_sentences = df_tokens\
        .groupby('sentence_id')\
        .token\
        .apply(lambda x:x.str.cat(sep=" "))\
        .reset_index()\
        .rename(columns={'token': 'text'})

    df_targets = df_targets.merge(df_tokens[['start', 'end']], left_index=True, right_index=True)
    df_targets = df_targets.merge(df_sentences, on='sentence_id')
    df_targets.date = df_targets.date.apply(lambda l: datetime.datetime.strftime(l, format="%Y-%m-%d"))

    if write2json:
        df_targets\
            .groupby('target')\
            .apply(
                lambda g: 
                    g.to_json(f'{parquet_filepath[:-8]}_{g.name}_target_usages.jsonl',
                              orient='records', lines=True, force_ascii=False),
                include_groups=False
            )

    return df_targets



if __name__ == '__main__':
    parser = argparse.ArgumentParser(
                    prog='Samples words from corpora',
                    description='Samples sentences given words and corpora')
    parser.add_argument('-t', '--target', nargs='+')
    parser.add_argument('-c', '--corpora', nargs='+')
    parser.add_argument('-o', '--output-folder', default="")
    parser.add_argument('-s', '--start')
    parser.add_argument('-e', '--end')

    args = parser.parse_args()
    corpora = args.corpora
    output_folder = args.output_folder
    for c in corpora:
        df = extract_target_usages(
            c,
            targets=args.target,
            start_date=args.start,
            end_date=args.end
        )