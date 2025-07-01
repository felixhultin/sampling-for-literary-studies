"""
Samples given words from corpora

"""

import argparse

from sampling import sample_data

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
                    prog='Samples words from corpora',
                    description='Samples sentences given words and corpora')
    parser.add_argument('-w', '--word', nargs='+')
    parser.add_argument('-c', '--corpora', nargs='+')
    parser.add_argument('-o', '--output-folder', default="")
    args = parser.parse_args()
    word = args.word
    corpora = args.corpora
    output_folder = args.output_folder
    if output_folder and not output_folder.endswith("/"):
        output_folder += "/"
    sample_data(corpora, word, output_folder=output_folder)

