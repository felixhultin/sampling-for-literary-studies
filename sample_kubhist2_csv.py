import datetime
import gzip

import pandas as pd

import pyarrow.csv as pv
import pyarrow.parquet as pq


def timeperiod_in_range(start, end, x, format = "%Y-%m-%d"):
    start_timestamp = datetime.datetime.strptime(start, format)
    end_timestamp = datetime.datetime.strptime(end, format)
    x_timestamp = datetime.datetime.strptime(x, format)
    if start_timestamp <= end_timestamp:
        return start_timestamp <= x_timestamp <= end_timestamp
    else:
        return start_timestamp <= x_timestamp or x <= end_timestamp

targets = set(['skär_JJ', 'telefon_NN'])
start = "1880-01-01"
end = "1883-12-31"
start_timestamp = pd.Timestamp(start).date()
end_timestamp = pd.Timestamp(end).date()
format = "%Y-%m-%d"
start_datetime = datetime.datetime.strptime(start, format)
end_datetime = datetime.datetime.strptime(end, format)
chunksize = 10 ** 6
occurences = []





parquet_file = pq.ParquetFile('kubhist2-aftonbladet-1880.parquet')
nof_rows_read = 0
target_words, target_pos = zip(*[t.split('_') for t in targets])
expression = '|'.join(target_words)
occurences, sentences = [], []
for i in parquet_file.iter_batches(batch_size=chunksize):
    chunk = i.to_pandas()
    if chunk.iloc[0].date >= end_timestamp:
        break
    elif chunk.iloc[0].date <= start_timestamp and chunk.iloc[-1].date <= end_timestamp:
        pass
    else:
        chunk_occurences = chunk[
            (chunk['lemma'].str.contains(expression))
        ]
        chunk_occurences.loc[:, 'lemma'] =  chunk_occurences.lemma.str.split('|')
        chunk_occurences = chunk_occurences.explode('lemma')
        chunk_occurences['target'] = chunk_occurences['lemma'] + '_' + chunk_occurences['pos']
        chunk_occurences = chunk_occurences[chunk_occurences['target'].isin(targets)]
        chunk_occurences = chunk_occurences[
            (chunk_occurences['date'] >= start_timestamp) & (chunk_occurences['date'] <= end_timestamp) 
        ]
        sentence_ids = chunk_occurences.sentence_id.unique()
        chunk_sentences = chunk[chunk['sentence_id'].isin(sentence_ids)]
        occurences.append(chunk_occurences)
        sentences.append(chunk_sentences)
    nof_rows_read += chunksize
    print(f"{nof_rows_read} lines read.")

df_occurences = pd.concat(occurences)
df_sentences = pd.concat(sentences)
df_sentence_id_sentence = df_sentences\
    .groupby('sentence_id')\
    .token\
    .apply(lambda x:x.str.cat(sep=" "))\
    .reset_index()\
    .rename(columns={'token': 'sentence'})
df_target_usages = df_occurences.merge(df_sentence_id_sentence, on='sentence_id')



# with pd.read_parquet('kubhist2-aftonbladet-1880.parquet', filters=[('date', '>=', start), ('date', '<=', end)], chunksize=chunksize) as reader:
#     nof_rows_read = 0
#     target_words, target_pos = zip(*[t.split('_') for t in targets])
#     expression = '|'.join(target_words)
#     for chunk in reader:
#         chunk_occurences = chunk[
#             (chunk['lemma'].str.contains(expression))
#         ]
#         chunk_occurences.loc[:, 'lemma'] =  chunk_occurences.lemma.str.split('|')
#         chunk_occurences = chunk_occurences.explode('lemma')
#         chunk_occurences['target'] = chunk_occurences['lemma'] + '_' + chunk_occurences['pos']
#         chunk_occurences = chunk_occurences[chunk_occurences['target'].isin(targets)]
#         sentence_ids = chunk_occurences.sentence_id.unique()
#         chunk_sentences = chunk[chunk['sentence_id'].isin(sentence_ids)]
#         chunk_sentences = chunk_sentences[
#             (chunk_sentences['date'].apply(lambda d: timeperiod_in_range(start, end, d)))
#         ]
#         nof_rows_read += chunksize
#         print(f"{nof_rows_read} lines read.")
        
#         sentence_ids = set
#     # Extract sentences from token occurences
# df_occurences = pd.concat(occurences)


# with pd.read_csv('kubhist2-aftonbladet-1880.tsv.gz', sep='\t', parse_dates=['date'], chunksize=chunksize) as reader:
#     nof_rows_read = 0
#     target_words, target_pos = zip(*[t.split('_') for t in targets])
#     expression = '|'.join(target_words)
#     for chunk in reader:
#         chunk_occurences = chunk[
#             (chunk['lemma'].str.contains(expression))
#         ]
#         chunk_occurences.loc[:, 'lemma'] =  chunk_occurences.lemma.str.split('|')
#         chunk_occurences = chunk_occurences.explode('lemma')
#         chunk_occurences['target'] = chunk_occurences['lemma'] + '_' + chunk_occurences['pos']
#         chunk_occurences = chunk_occurences[chunk_occurences['target'].isin(targets)]
#         sentence_ids = chunk_occurences.sentence_id.unique()
#         chunk_sentences = chunk[chunk['sentence_id'].isin(sentence_ids)]
#         # chunk_sentences = chunk_sentences[
#         #     (chunk_sentences['date'].apply(lambda d: timeperiod_in_range(start, end, d)))
#         # ]
#         nof_rows_read += chunksize
#         print(f"{nof_rows_read} lines read.")
        
#         sentence_ids = set
#     # Extract sentences from token occurences
# df_occurences = pd.concat(occurences)