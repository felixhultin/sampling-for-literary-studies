#!/bin/bash

target_words=($(csvcut mtp_target_words.tsv -t -c "words,PoS" | tr "," "_" | tail -n +2))
echo "Uses following target words: ${target_words[@]}"
# Sampling for t1, phase 1
kubhist2_1880=($(ls -d data/corpora/kubhist2/*1880*.xml.bz2))
echo "Starting to process the following files: ${kubhist2_1880[@]}"
python sample_kubhist2.py \
    -w ${target_words[@]} \
    -c ${kubhist2_1880[@]} \
    -s "1880-01-01" \
    -e "1883-12-31" \
    -o "t1_kubhist"

# Sampling for t9, phase 1
kubhist=($(ls -d data/corpora/kubhist)) # no need to filter, only t9 in folder
echo "Starting to process the following files: ${kubhist[@]}"
python sample_kubhist2.py \
    -w ${target_words[@]} \
    -c ${kubhist[@]} \
    -s "1918-01-01" \
    -e "1922-12-31"
