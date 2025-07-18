import argparse
import bz2
import csv
import gzip

import lxml.etree as ET

from pathlib import Path

def open_file(file_path, mode='rb'):
    if file_path.endswith('.bz2'):
        return bz2.open(file_path, mode=mode)
    else:
        return open(file_path, mode=mode)

def xml2csv(xml_path):
    csv_path = xml_path[:-8] + '.tsv.gz' # excludes the '.xml.bz2' part
    csv_path = Path(csv_path).name
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
    

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-x", "--xml-file")
    args = parser.parse_args()
    xml2csv(args.xml_file)
