import bz2
import jsonlines
import datetime
import logging

import requests
import lxml.etree as ET

from pathlib import Path
from typing import List
from tqdm import tqdm

logging.basicConfig(level='INFO')



SBX_METADATA_API_ENDPOINT = "https://ws.spraakbanken.gu.se/ws/metadata/v3/"


def open_file(file_path, mode='r', encoding='utf-8'):
    if file_path.endswith('.bz2'):
        return bz2.open(file_path, mode=mode)#, encoding=encoding)
    else:
        return open(file_path, mode=mode)#, encoding=encoding)


def count_tags(xml_file, tag: str = 'text'):
    count = 0
    for _, elem in ET.iterparse(xml_file, events=('end',)):
        if elem.tag == tag:
            count += 1
            elem.clear()
    # try:
    #     for _, elem in ET.iterparse(xml_file, events=('end',)):
    #         if elem.tag == 'sentence':
    #             count += 1
    #             elem.clear()
    # except xml.etree.ElementTree.ParseError:
    #     return
    return count

def _fetch_metadata(api_endpoint : str, resource_name: str):
    metadata_query = f"{api_endpoint}?resource={resource_name}"
    logging.info(f"Fetching metadata from {metadata_query}")
    try:
        resp = requests.get(metadata_query)
        resp.raise_for_status()
    except requests.exceptions.HTTPError as err:
        logging.info("Fatal error: could not fetch metadata from metadata api")
        raise SystemExit(err)
    metadata = resp.json()
    return metadata

def get_nof_sentences_from_resource(resource_name: str):
    metadata = _fetch_metadata(SBX_METADATA_API_ENDPOINT, resource_name)
    print(metadata)
    return metadata['size']['sentences']

def timeperiod_in_range(start, end, x, format = "%Y-%m-%d"):
    start_timestamp = datetime.datetime.strptime(start, format)
    end_timestamp = datetime.datetime.strptime(end, format)
    x_timestamp = datetime.datetime.strptime(x, format)
    if start_timestamp <= end_timestamp:
        return start_timestamp <= x_timestamp <= end_timestamp
    else:
        return start_timestamp <= x_timestamp or x <= end_timestamp


def extract_sentences_with_lemma(xml_file, output_file, targets : set[tuple], start_date: str, end_date: str):
    if type(targets) != set:
        raise ValueError("targets must be a set for processing purposes")
    resource_name = Path(Path(xml_file).stem).stem
    total_sentences = get_nof_sentences_from_resource(resource_name)
    with open_file(xml_file) as f_in, jsonlines.open(f'{resource_name}_target_usages.jsonl', mode='w') as writer, tqdm(total=total_sentences, desc="Processing") as pbar:
        context = ET.iterparse(f_in, events=('start', 'end'))  
        for event, elem in context:
            if event == 'start':
                if elem.tag == 'text':
                    text_date = elem.get('date')
                    process_current_text = timeperiod_in_range(start_date, end_date, text_date)
                if elem.tag == 'sentence' and process_current_text:
                    offsets, matches = [], []
                    sentence = ""
                if elem.tag == 'token' and process_current_text:
                    text = elem.text if elem.text is not None else "<PARSE_ERROR>"
                    lemmas_string = elem.get('lemma')
                    pos = elem.get('pos')
                    lemmas = [l for l in lemmas_string.split('|')]
                    for lemma in lemmas: 
                        if (lemma, pos) in targets:
                            start = len(sentence)
                            end = len(sentence) + len(text) - 1
                            offsets.append( (start, end) )
                            matches.append(lemma)
                    sentence += text + " "
                        
            elif event == 'end':
                if elem.tag == 'sentence' and process_current_text:
                    # Remove trailing space
                    sentence = sentence[:-1]
                    for off, match in zip(offsets, matches):
                        start, end = off
                        writer.write({
                            'id': elem.get('id'),
                            'start': start,
                            'end': end,
                            'target': match,
                            'sentence': sentence
                        })
                    pbar.update(1)

                # Clear memory
                elem.clear()
                parent = elem.getparent()
                if parent is not None:
                    while elem.getprevious() is not None:
                        del parent[0]
        del context


if __name__ == '__main__':
    #paths = ['data/corpora/kubhist2/kubhist2-falkopingstidning-1880.xml.bz2', 'data/corpora/kubhist2/kubhist2-falkopingstidning-1890.xml.bz2']
    #for p in paths:
    #    f = open_file(p)
    extract_sentences_with_lemma(
        #'data/corpora/svt/svt-2009.xml.bz2',
        'data/corpora/kubhist2/kubhist2-fahluweckoblad-1810.xml.bz2',
        #'data/corpora/kubhist2/kubhist2-carlscronaswekoblad-1780.xml.bz2', 
        'output.jsonl', 
        targets={
            ('till', 'PP')
        }, 
        start_date='1810-01-01', 
        end_date='1820-12-31'
    )
