import argparse
import bz2
import jsonlines
import datetime
import logging

import requests
import lxml.etree as ET

from pathlib import Path
from tqdm import tqdm

logging.basicConfig(level='INFO')

SBX_METADATA_API_ENDPOINT = "https://ws.spraakbanken.gu.se/ws/metadata/v3/"


def open_file(file_path, mode='rb'):
    if file_path.endswith('.bz2'):
        return bz2.open(file_path, mode=mode)
    else:
        return open(file_path, mode=mode)

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
    return metadata['size']['sentences']

def timeperiod_in_range(start, end, x, format = "%Y-%m-%d"):
    start_timestamp = datetime.datetime.strptime(start, format)
    end_timestamp = datetime.datetime.strptime(end, format)
    x_timestamp = datetime.datetime.strptime(x, format)
    if start_timestamp <= end_timestamp:
        return start_timestamp <= x_timestamp <= end_timestamp
    else:
        return start_timestamp <= x_timestamp or x <= end_timestamp


def extract_target_usages(xml_file, targets : set[tuple], start_date: str, end_date: str):
    if type(targets) != set:
        raise ValueError("targets must be a set for processing purposes")
    resource_name = Path(Path(xml_file).stem).stem
    total_sentences = get_nof_sentences_from_resource(resource_name)
    logging.info(f"Start processing: {xml_file}")
    if args.output_folder:
        output_folder = args.output_folder
    else:
        output_folder = 'output'
    Path(output_folder).mkdir(parents=True, exist_ok=True)
    with open_file(xml_file) as f_in, tqdm(total=total_sentences, desc="Processing") as pbar:
        target_json_writer = {
            (t,pos): jsonlines.open(f'{output_folder}/{resource_name}_{t}_{pos}_target_usages.jsonl', mode='w') for t, pos in targets
        }
        context = ET.iterparse(f_in, events=('start', 'end'))
        for event, elem in context:
            if event == 'start':
                if elem.tag == 'text':
                    text_date = elem.get('date')
                    process_current_text = timeperiod_in_range(start_date, end_date, text_date)
                if elem.tag == 'sentence' and process_current_text:
                    offsets, matches, pos_tags = [], [], []
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
                            pos_tags.append(pos)
                    sentence += text + " "
                        
            elif event == 'end':
                if elem.tag == 'sentence':
                    if process_current_text:
                        sentence = sentence[:-1] # Remove trailing space
                        for off, match, pos_tag in zip(offsets, matches, pos_tags):
                            start, end = off
                            writer = target_json_writer[match, pos_tag]
                            writer.write({
                                'id': elem.get('id'),
                                'start': start,
                                'end': end,
                                'target': match,
                                'pos_tag': pos_tag,
                                'sentence': sentence,
                                'date': text_date
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
    parser = argparse.ArgumentParser(
                    prog='Samples words from corpora',
                    description='Samples sentences given words and corpora')
    parser.add_argument('-w', '--word', nargs='+') # W/o nargs='+' because of spaces.
    parser.add_argument('-c', '--corpora', nargs='+')
    parser.add_argument('-o', '--output-folder', default="")
    parser.add_argument('-s', '--start')
    parser.add_argument('-e', '--end')

    args = parser.parse_args()
    corpora = args.corpora
    output_folder = args.output_folder
    targets = []
    for idx, w in enumerate(args.word):
        try:
            splitted = w.split("_")
            # For edge case with verb and preposition, for example: 'backa upp'
            if len(splitted) == 3:
                verb, preposition, pos = splitted
                w = f"{verb} {preposition}"
                args.word[idx] = f"{w}_{pos}"
            else:
                w, pos = splitted
        except:
            raise ValueError("-w or --word must of format <word>_<part-of-speech>")

    targets = {tuple(w.split("_")) for w in args.word}
    for c in corpora:
        extract_target_usages(
            c,
            targets=targets,
            start_date=args.start,
            end_date=args.end
        )
