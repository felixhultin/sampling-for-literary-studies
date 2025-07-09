target_words=($(csvcut mtp_target_words.tsv -t -c "words,PoS" | tr "," "_" | tail -n +2))
echo "Uses following target words: ${target_words[@]}"

# Sampling for t1, phase 1
python sample_kubhist2.py \
    -w ${target_words[@]} \
    -c data/corpora/svt/svt-2006.xml.bz2 data/corpora/svt/svt-2007.xml.bz2 \
    -s "2006-01-01" \
    -e "2007-12-31" \
    -o "t1_svt"

# Sampling for t9, phase 1
python sample_kubhist2.py \
    -w ${target_words[@]} \
    -c data/corpora/svt/svt-2022.xml.bz2 data/corpora/svt/svt-2023.xml.bz2 \
    -s "2022-01-01" \
    -e "2023-12-31" \
    -o "t9_svt"