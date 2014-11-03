#!/bin/bash
# Runs a single k-means and fuzzy k-means mahout clustering job

#Arguments:
# $1 = Convergence Delta (-cd) (used to be 0.05)
# $2 = Maximum number of iterations (used to be 10)
# $3 = ngram number (can be 1,2,3, or 4)
# $4 = Fuzziness factor f s.t. 1 <= f <= 2, where 1 is the least "fuzzy" (i.e. closest to k-means)

# Run Normal kmeans
printf "EXECUTING K-MEANS CLUSTERING WITH PARAMETERS: {cd: $1, x: $2, ngram: $3}\n"
mahout/bin/mahout kmeans -i filtered_tfidf/l2_$3gram/tfidf-vectors/ -c clusters_1 -o clustering_output/kmeans_cd$1_x$2_$3gram -cd $1 -x $2 -k 10 -cl -ow

# Dump normal kmeans output
printf "DUMPING K-MEANS CLUSTERING WITH PARAMETERS: {cd: $1, x: $2, ngram: $3}\n\n"
mahout/bin/mahout clusterdump -d filtered_tfidf/l2_$3gram/dictionary.file-0 -dt sequencefile -i clustering_output/kmeans_cd$1_x$2_$3gram/clusters-2-final -n 10 -b 100 -o clusterdump_output/kmeans_cd$1_x$2_$3gram

# Run Fuzzy kmeans
printf "EXECUTING FUZZY K-MEANS CLUSTERING OUTPUT WITH PARAMETERS: {cd: $1, x: $2, ngram: $3, m: $4}\n"
mahout/bin/mahout fkmeans -i filtered_tfidf/l2_$3gram/tfidf-vectors/ -c clusters_1 -o clustering_output/fkmeans_cd$1_x$2_$3gram_m$4 -cd $1 -x $2 -k 10 -cl -ow -m $4

# Dump Fuzzy means output
printf "EXECUTING FUZZY K-MEANS CLUSTERING WITH PARAMETERS: {cd: $1, x: $2, ngram: $3, m: $4}\n\n"
mahout/bin/mahout clusterdump -d filtered_tfidf/l2_$3gram/dictionary.file-0 -dt sequencefile -i clustering_output/fkmeans_cd$1_x$2_$3gram_m$4/clusters-2-final -n 10 -b 100 -o clusterdump_output/fkmeans_cd$1_x$2_$3gram_m$4