#!/bin/sh
# This is a comment

#Arguments:
# $1 = Convergnce Delta (-cd) (used to be 0.05)
# $2 = Number of iterations (used to be 10)
# $3 = ngram number (can be 1,2,3, or 4)
# $4 = Test number (the file location will be saved as eden_clustering_output/kmeans_run_$1)

# Run Normal kmeans
echo "EXECUTING K-MEANS CLUSTERING WITH PARAMETERS: Convergence Delta - $1 Number of Iterations - $2"
mahout/bin/mahout kmeans -i filtered_tfidf/l2_$3gram/tfidf-vectors/ -c clusters_1 -o eden_clustering_output/kmeans_run_$4_$3_ngrams -cd $1 -x $2 -k 10 -cl -ow

# Run Fuzzy kmeans
echo "EXECUTING FUZZY K-MEANS CLUSTERING WITH PARAMETERS: Convergence Delta - $1 Number of Iterations - $2"
mahout/bin/mahout fkmeans -i filtered_tfidf/l2_$3gram/tfidf-vectors/ -c clusters_1 -o eden_clustering_output/fkmeans_run_$4_$3_ngrams -cd $1 -x $2 -k 10 -cl -ow	

# Dump normal kmeans output
mahout/bin/mahout clusterdump -d filtered_tfidf/l2_$3gram/dictionary.file-0 -dt sequencefile -i eden_clustering_output/kmeans_run_$4_$3_ngrams/clusters-2-final -n 10 -b 100 -o clusterdump_output/kmeans_run_$4_$3_ngrams_clusterdump_output

# Dump Fuzzy means output
mahout/bin/mahout clusterdump -d filtered_tfidf/l2_$3gram/dictionary.file-0 -dt sequencefile -i eden_clustering_output/fkmeans_run_$4_$3_ngrams/clusters-2-final -n 10 -b 100 -o clusterdump_output/fkmeans_run_$4_$3_ngrams_clusterdump_output
