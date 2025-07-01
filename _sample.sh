#!/bin/bash

svt_corpora=(
    "data/corpora/svt/svt-2007.xml.bz2"
    "data/corpora/svt/svt-2008.xml.bz2"
    "data/corpora/svt/svt-2009.xml.bz2"
    "data/corpora/svt/svt-2010.xml.bz2"
    "data/corpora/svt/svt-2011.xml.bz2"
    "data/corpora/svt/svt-2012.xml.bz2"
    "data/corpora/svt/svt-2013.xml.bz2"
    "data/corpora/svt/svt-2014.xml.bz2"
    "data/corpora/svt/svt-2015.xml.bz2"
    "data/corpora/svt/svt-2016.xml.bz2"
    "data/corpora/svt/svt-2017.xml.bz2"
    "data/corpora/svt/svt-2018.xml.bz2"
    "data/corpora/svt/svt-2019.xml.bz2"
    "data/corpora/svt/svt-2020.xml.bz2"
    "data/corpora/svt/svt-2021.xml.bz2"
    "data/corpora/svt/svt-2022.xml.bz2"
    "data/corpora/svt/svt-2023.xml.bz2"
)

python sample.py -w virus viral pandemi -c "${svt_corpora[@]}"
#python sample.py -w telefon krig försvar -c "${kubhist2_corpora[@]}"

# telefon
# krig
# försvar
# adress
# förälder
# check 
# klimat
# marknad/en
# AI
# gäng
# export
# sändning
# pappa/far
# kod
# mus
# program
# energi
# racism
# väst/öst
# minne
# skär
# rappare
# knark
# brud
# panel
# dum
# mobil
# aktör
# mask
# mussla
# post
# samtal
# partner
# fru
# flicka
# migration
# migrant
# invandrare
# suger