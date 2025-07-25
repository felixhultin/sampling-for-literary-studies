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

targets = set(['skär_JJ', 'telefon_NN', 'fru_NN', 'herre_NN', 'pappa_NN'])
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
        sentence_tokens.append(chunk_sentences)
    nof_rows_read += chunksize
    print(f"{nof_rows_read} lines read.")

df_targets = pd.concat(occurences)
df_tokens = pd.concat(sentence_tokens)

# Compute length of each token
df_tokens['length'] = df_tokens['token'].str.len()

# Add a helper column for the space after each token (except the last token in sentence)
df_tokens['space'] = 1  # space after token
# We will remove the space after the last token in each sentence
is_last = df_tokens['sentence_id'] != df_tokens['sentence_id'].shift(-1)
df_tokens.loc[is_last, 'space'] = 0

# Calculate cumulative start position within each sentence
df_tokens['start'] = \
    df_tokens.groupby('sentence_id')[['length', 'space']].cumsum()['length'] - \
    df_tokens['length'] + df_tokens.groupby('sentence_id')['space'].cumsum() - \
    df_tokens['space']

# End is start + length
df_tokens['end'] = df_tokens['start'] + df_tokens['length']

# Drop helper columns if needed
df_tokens = df_tokens.drop(columns=['length', 'space'])


df_sentences = df_tokens\
    .groupby('sentence_id')\
    .token\
    .apply(lambda x:x.str.cat(sep=" "))\
    .reset_index()\
    .rename(columns={'token': 'text'})

df_targets = df_targets.merge(df_tokens[['start', 'end']], left_index=True, right_index=True)
df_targets = df_targets.merge(df_sentences, on='sentence_id')


def extract_target_usages_pd(df_tokens : pd.DataFrame, df_targets : list[str]):
    targets = set(zip(df_targets['lemma'], df_targets['pos']))
    df_sentences = df_tokens.groupby('sentence_id')


    pass

def extract_target_usages_loop(df_tokens : pd.DataFrame, df_targets : list[str]):
    """Loops through a dataframe of tokens for matches of target tokens"""
    targets = set(zip(df_targets['lemma'], df_targets['pos']))
    df_sentences = df_tokens.groupby('sentence_id')
    jsonl = []
    for sentence_id, df_sent in df_sentences:
        sentence = ""
        offsets, matches, pos_tags = [], [], []
        for _, row in df_sent.iterrows():
            text = row.token
            if type(text) != str:
                text = '<PARSE_ERROR>'
            pos = row.pos
            lemmas_string = row.lemma
            if lemmas_string == '|':
                if (text, pos) in targets:
                    start = len(sentence)
                    end = len(sentence) + len(text)
                    offsets.append( (start, end) )
                    matches.append(text)
                    pos_tags.append(pos)
            else:
                lemmas = [l for l in lemmas_string.split('|')]
                for lemma in lemmas:
                    if (lemma, pos) in targets:
                        start = len(sentence)
                        end = len(sentence) + len(text)
                        offsets.append( (start, end) )
                        matches.append(lemma)
                        pos_tags.append(pos)
            sentence += text + " "

        sentence = sentence[:-1] # Remove trailing space
        for off, match, pos_tag in zip(offsets, matches, pos_tags):
            start, end = off
            jsonl.append({
                'idx': sentence_id,
                'text': sentence,
                'target': match,
                'pos_tag': pos_tag,
                'start': start,
                'end': end
            })
    return pd.DataFrame.from_records(jsonl)