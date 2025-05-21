import logging
import pandas as pd

from pathlib import Path
from pprint import pformat
from typing import List

from languagechange.corpora import Corpus, SprakBankenCorpus
from languagechange.search import SearchTerm


def extract_search_terms_from_wordlist(wordlist: List[str]):
    search_terms = []
    for l in list(dict.fromkeys(wordlist)):
        st = SearchTerm(l, word_feature = 'lemma')
        search_terms.append(st)
    return search_terms


def sample_words_from_corpora(corpora: List[Corpus], search_terms : List[SearchTerm], output_folder :str = ''):
    for corpus in corpora:
        usage_dictionary = corpus.search(search_terms)
        filename_wo_extension = Path(corpus.name).stem.split('.')[0]
        usage_dictionary.save(output_folder + filename_wo_extension)


def sample_data(
        excel_path : str,
        sheet_name : str,
        corpora : List[Corpus],
        output_folder : str,
        wordfeature_col : str = 'lemma',
    ):
    logging.info(f"Reading column {wordfeature_col} at from {excel_path}")
    df = pd.read_excel(excel_path, engine="odf", sheet_name = sheet_name)
    wordlist = df[wordfeature_col]
    logging.info(f"Sampling the following words:\n")
    logging.info(pformat(list(wordlist)))
    search_terms = extract_search_terms_from_wordlist(wordlist)
    sample_words_from_corpora(corpora, search_terms, output_folder=output_folder)


if __name__ == '__main__':
    sample_data(
        "data/words2sample/Words2SampleOverview.ods",
        "Political words (Miriam)",
        corpora = [
            SprakBankenCorpus('data/corpora/svt/svt-2007.xml.bz2'),
            SprakBankenCorpus('data/corpora/svt/svt-2008.xml.bz2'),
            SprakBankenCorpus('data/corpora/svt/svt-2009.xml.bz2'),
            SprakBankenCorpus('data/corpora/svt/svt-2010.xml.bz2'),
            SprakBankenCorpus('data/corpora/svt/svt-2011.xml.bz2'),
            SprakBankenCorpus('data/corpora/svt/svt-2012.xml.bz2'),
            SprakBankenCorpus('data/corpora/svt/svt-2013.xml.bz2'),
            SprakBankenCorpus('data/corpora/svt/svt-2014.xml.bz2'),
            SprakBankenCorpus('data/corpora/svt/svt-2015.xml.bz2'),
            SprakBankenCorpus('data/corpora/svt/svt-2016.xml.bz2'),
            SprakBankenCorpus('data/corpora/svt/svt-2017.xml.bz2'),
            SprakBankenCorpus('data/corpora/svt/svt-2018.xml.bz2'),
            SprakBankenCorpus('data/corpora/svt/svt-2019.xml.bz2'),
            SprakBankenCorpus('data/corpora/svt/svt-2020.xml.bz2'),
            SprakBankenCorpus('data/corpora/svt/svt-2021.xml.bz2'),
            SprakBankenCorpus('data/corpora/svt/svt-2022.xml.bz2'),
            SprakBankenCorpus('data/corpora/svt/svt-2023.xml.bz2')
        ],
        output_folder = 'data/outputs/',
        wordfeature_col = 'lemma'
    )
    sample_data(
        "data/words2sample/Words2SampleOverview.ods",
        "Gender studies (Mia)",
        corpora = [
            SprakBankenCorpus('data/corpora/parliament/rd-anf-1993-2018.xml.bz2', token_tag='w')
        ],
        output_folder = 'data/outputs/',
        wordfeature_col = 'Swedish – lemmas'
    )

    sample_data(
        "data/words2sample/Words2SampleOverview.ods",
        "Electricity (Mats)",
        corpora = [
            SprakBankenCorpus('data/corpora/kubhist2/kubhist2-falkopingstidning-1880.xml.bz2'),
            SprakBankenCorpus('data/corpora/kubhist2/kubhist2-falkopingstidning-1890.xml.bz2')

        ],
        output_folder = 'data/outputs/',
        wordfeature_col = 'Swedish – lemmas'
    )