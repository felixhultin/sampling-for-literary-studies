#!/bin/bash

# Sampling for t1, phase 1
kubhist_1880=($(ls -d data/corpora/kubhist2/*1880*.xml.bz2))
echo "Starting to process the following files: ${kubhist_1880[@]}"
python sample_kubhist2.py \
    -w fru_NN fruga_NN husmoder_NN \
    -c ${kubhist_1880[@]} \
    -s "1880-01-01" \
    -e "1883-12-31"

# Sampling for t9, phase 1
kubhist_1910=($(ls -d data/corpora/kubhist2/*1910*.xml.bz2))
kubhist_1920=($(ls -d data/corpora/kubhist2/*1920*.xml.bz2))
kubhist_1910_1920=("${kubhist_1910[@]}" "${kubhist_1920[@]}")
echo "Starting to process the following files: ${kubhist_1910_1920[@]}"
python sample_kubhist2.py \
    -w fru_NN fruga_NN husmoder_NN \
    -c ${kubhist_1910_1920[@]} \
    -s "1918-01-01" \
    -e "1922-12-31"
