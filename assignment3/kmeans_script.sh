#!/bin/sh
# This is a comment

#Arguments:
# $1 = Convergnce Delta (-cd) (used to be 0.05)
# $2 = Number of iterations (used to be 10)
# $3 = Test number (the file location will be saved as eden_clustering_output/kmeans_run_$1)
# $4 = ngram number (can be 1,2,3, or 4)

echo "EXECUTING CLUSTERING WITH PARAMETERS: Convergence Delta - $1 Number of Iterations - $2"
mahout/bin/mahout kmeans -i filtered_tfidf/l2_$4gram/tfidf-vectors/ -c clusters_1 -o eden_clustering_output/kmeans_run_$3_$4_ngrams -cd $1 -x $2 -k 10 -cl -ow

mahout/bin/mahout clusterdump -d filtered_tfidf/l2_$4gram/dictionary.file-0 -dt sequencefile -i eden_clustering_output/kmeans_run_$3/clusters-2-final -n 10 -b 100 -o clusterdump_output/k_means_run_$3_$4_ngrams_clusterdump_output