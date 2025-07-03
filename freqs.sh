#!/bin/bash

declare -a array=(
    "virus" \
    "viral" \
    "pandemi" \
    "telefon" \
    "krig" \
    "försvar" \
    "adress" \
    "förälder" \
    "check" \
    "klimat" \
    "marknad/en" \
    "AI" \
    "gäng" \
    "export" \
    "sändning" \
    "pappa" \
    "far" \
    "kod" \
    "mus" \
    "program" \
    "energi" \
    "racism" \
    "väst" \
    "öst" \
    "minne" \
    "skär" \
    "rappare" \
    "knark" \
    "brud" \
    "panel" \
    "dum" \
    "mobil" \
    "aktör" \
    "mask" \
    "mussla" \
    "post" \
    "samtal" \
    "partner" \
    "fru" \
    "flicka" \
    "migration" \
    "migrant" \
    "invandrare" \
    "suger" \
)

output_path="word_freqs.txt"
echo "file,word,count" >> "$output_path"
for w in "${array[@]}"
do
   for file in data/corpora/svt/stats*.csv; do        
        { echo "$file, $w, "; csvgrep -t -c "lemma" -r "^$w$" "$file" | csvcut -c "count" | csvstat --sum;} | tr -d '\n'  >> "$output_path"
        echo >> "$output_path"
   done
done

