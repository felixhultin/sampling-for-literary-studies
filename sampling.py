import os
import pandas as pd


from pathlib import Path
from typing import List

from languagechange.corpora import Corpus, SprakBankenCorpus
from languagechange.search import SearchTerm


def extract_search_terms_from_wordlist(wordlist: List[str]):
    search_terms = []
    for l in wordlist:
        st = SearchTerm(l, word_feature = 'lemma')
        search_terms.append(st)
    return search_terms


def sample_words_from_corpora(corpora: List[Corpus], search_terms : List[SearchTerm], output_folder :str = ''):
    for corpus in corpora:
        usage_dictionary = corpus.search(search_terms)
        filename_wo_extension = Path(corpus.name).stem
        usage_dictionary.save(output_folder + filename_wo_extension)


def sample_data(excel_path : str, sheet_name : str, filepaths : List | str, output_folder : str, corpus_class : Corpus):
    df = pd.read_excel(excel_path, engine="odf", sheet_name = sheet_name)
    search_terms = extract_search_terms_from_wordlist(df.lemma)
    sample_words_from_corpora([corpus_class(c) for c in filepaths[:1]], search_terms, output_folder=output_folder)



if __name__ == '__main__':
    sample_data(
        "data/words2sample/Words2SampleOverview.ods",
        "Political words (Miriam)",
        filepaths = [
            'data/corpora/svt/svt-2007.xml.bz2',
            'data/corpora/svt/svt-2008.xml.bz2',
            'data/corpora/svt/svt-2009.xml.bz2',
            'data/corpora/svt/svt-2010.xml.bz2'
            'data/corpora/svt/svt-2011.xml.bz2',
            'data/corpora/svt/svt-2012.xml.bz2',
            'data/corpora/svt/svt-2013.xml.bz2',
            'data/corpora/svt/svt-2014.xml.bz2',
            'data/corpora/svt/svt-2015.xml.bz2',
            'data/corpora/svt/svt-2016.xml.bz2',
            'data/corpora/svt/svt-2017.xml.bz2',
            'data/corpora/svt/svt-2018.xml.bz2',
            'data/corpora/svt/svt-2019.xml.bz2',
            'data/corpora/svt/svt-2020.xml.bz2',
            'data/corpora/svt/svt-2021.xml.bz2',
            'data/corpora/svt/svt-2022.xml.bz2',
            'data/corpora/svt/svt-2023.xml.bz2'
        ],
        output_folder='data/outputs/',
        corpus_class = SprakBankenCorpus
    )
    # sample_data(
    #     "data/words2sample/Words2SampleOverview.ods",
    #     "Gender studies (Mia)",
    #     input_folder = 'data/corpora/parliament',
    #     output_folder='data/outputs/'
    # )

    # sample_data(
    #     "data/words2sample/Words2SampleOverview.ods",
    #     "Electricity (Mats)",
    #     input_folder = 'data/corpora/svt',
    #     output_folder='data/outputs/'
    # )

    # df = pd.read_excel("data/words2sample/Words2SampleOverview.ods", engine="odf", sheet_name = "Political words (Miriam)")
    # search_terms = extract_search_terms_from_wordlist(df.lemma)
    # folder_name = 'data/corpora/svt'
    # filepaths = [f'{folder_name}/{c}' for c in os.listdir(folder_name)]
    # filepaths = filepaths[-1:]
    # sample_words_from_corpora([SprakBankenCorpus(c) for c in filepaths], search_terms[:1])