"""

Samples given words from corpora


"""

import argparse

from sampling import sample_data

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
                    prog='Samples words from corpora',
                    description='Samples sentences given words and corpora')
    parser.add_argument('-w', '--word')
    parser.add_argument('-c', '--corpora', nargs='+')
    parser.add_argument('-o', '--output-folder')
    args = parser.parse_args()
    word = args.word
    corpora = args.corpora
    output_folder = args.output_folder
    sample_data(word, corpora, output_folder=output_folder)

