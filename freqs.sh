#!/bin/bash

target_words=($(csvcut mtp_target_words.tsv -t -c "words" | tail -n +2))
target_pos=($(csvcut mtp_target_words.tsv -t -c "PoS" | tail -n +2))
echo "${target_words[@]}"
echo "${target_pos[@]}"


output_path="word_freqs.txt"
echo "file,word,pos,count" >> "$output_path"

for file in data/corpora/svt/stats*.csv; do
    echo "Processing $file"
    for index in ${!target_words[*]}
    do
    w=${target_words[$index]}
    pos=${target_pos[$index]}
        {
            printf "%s,%s,%s," "$file" "$w" "$pos"
            csvgrep -t -c "lemma" -r "^$w$" "$file" \
                | csvgrep -c "POS" -r "^$pos" \
                | csvcut -c count \
                | tail -n +2 \
                | awk '{s+=$1} END {print (s == "" ? 0 : s)}' \
                | tr -d '\n'
        } >> "$output_path"
        echo >> "$output_path"
   done
done

