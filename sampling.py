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


def sample_words_from_corpora(corpora: List[Corpus], search_terms : List[SearchTerm]):
    for corpus in corpora:
        usage_dictionary = corpus.search(search_terms)
        filename_wo_extension = Path(corpus.name).stem
        usage_dictionary.save(filename_wo_extension)


if __name__ == '__main__':
    df = pd.read_excel("data/words2sample/Words2SampleOverview.ods", engine="odf", sheet_name = "Political words (Miriam)")
    search_terms = extract_search_terms_from_wordlist(df.lemma)
    folder_name = 'data/corpora/svt'
    filepaths = [f'{folder_name}/{c}' for c in os.listdir(folder_name)]
    filepaths = filepaths[-1:]
    sample_words_from_corpora([SprakBankenCorpus(c) for c in filepaths], search_terms[:1])