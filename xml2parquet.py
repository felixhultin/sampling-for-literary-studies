import argparse
import bz2
import csv
import gzip
import pyarrow as pa
import pyarrow.csv as pv
import pyarrow.parquet as pq

import lxml.etree as ET

from pathlib import Path

def open_file(file_path, mode='rb'):
    if file_path.endswith('.bz2'):
        return bz2.open(file_path, mode=mode)
    else:
        return open(file_path, mode=mode)

def xml2tsv(xml_path):
    csv_path = Path(xml_path).name.split('.')[0] + '.tsv.gz'
    with open_file(xml_path) as f_in, gzip.open(csv_path, 'wt', newline='') as csvfile:
        context = ET.iterparse(f_in, events=('start', 'end'))
        fieldnames = ['token', 'lemma', 'pos', 'sentence_id', 'date']
        writer = csv.DictWriter(csvfile, delimiter = '\t', fieldnames=fieldnames)
        writer.writeheader()
        for event, elem in context:
            if event == 'start':
                if elem.tag == 'text':
                    date = elem.get('date')
                if elem.tag == 'sentence':
                    sentence_id = elem.get('id')
                if elem.tag == 'token':
                    writer.writerow({
                        'token': elem.text,
                        'lemma': elem.get('lemma'),
                        'pos': elem.get('pos'),
                        'date': date,
                        'sentence_id': sentence_id
                    })
            elif event == 'end':
                # Clear memory
                elem.clear()
                parent = elem.getparent()
                if parent is not None:
                    while elem.getprevious() is not None:
                        del parent[0]
        del context
    return csv_path

def tsv2parquet(tsv_path, parquet_path, block_size=1 << 20, compression="snappy"):
    """
    Convert a large CSV file to Parquet format without loading it fully into memory.

    Parameters:
        tsv_path (str): Path to input CSV file.
        parquet_path (str): Path to output Parquet file.
        block_size (int): Number of bytes to read per block (default: 1 MB).
        compression (str): Parquet compression (default: 'snappy').
    """
    # Set up streaming CSV reader
    read_opts = pv.ReadOptions(block_size=block_size)
    parse_opts = pv.ParseOptions(delimiter='\t')
    convert_opts = pv.ConvertOptions()  # Can specify column types if known

    csv_reader = pv.open_csv(tsv_path, read_options=read_opts, convert_options=convert_opts, parse_options=parse_opts)

    writer = None
    for batch in csv_reader:
        table = pa.Table.from_batches([batch])  # Convert RecordBatch → Table
        if writer is None:
            writer = pq.ParquetWriter(parquet_path, batch.schema, compression=compression)
        writer.write_table(table)

    if writer:
        writer.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-x", "--xml-file")
    args = parser.parse_args()
    tsv_path = xml2tsv(args.xml_file)
    parquet_path = Path(tsv_path).name.split('.')[0] + '.parquet'
    tsv2parquet(tsv_path, parquet_path)
