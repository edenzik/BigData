#!/bin/bash
# Runs a single k-means and fuzzy k-means mahout clustering job

#Arguments:
# $1 = Convergence Delta (-cd) (used to be 0.05)
# $2 = Maximum number of iterations (used to be 10)
# $3 = ngram number (can be 1,2,3, or 4)
# $4 = Fuzziness factor f s.t. 1 <= f <= 2, where 1 is the least "fuzzy" (i.e. closest to k-means)
# $5= Distance Measure (Default is Euclidian, but you can pass "Cosine", "Manhattan", etc.)

# Pattern for default parameter values found at:
# http://jaduks.livejournal.com/7934.html

# Sets convergence delta to be arg 1 or .05 if no argument is passed
CD=${1:-".05"}

# Sets max # iterations to be arg 2 or 10 if no argument is passed
X=${2:-"10"}

# Sets ngram number to be arg 3 or 1 if no argument is passed
NGRAM=${3:-"1"}

# Sets fuzziness factor to be arg 4 or 1.1 if no argument is passed
M=${4:-"1.1"}

# Sets distance measure to be argument 5 or "Euclidian" if no argument is passed
DM_SHORT=${5:-"SquaredEuclidean"}
DM=org.apache.mahout.common.distance.${DM_SHORT}DistanceMeasure

KMEANS_PATH=kmeans_cd${CD}_x${X}_${NGRAM}gram_dm${DM_SHORT}
FKMEANS_PATH=f${KMEANS_PATH}_m${M}

# Run Normal kmeans and log output in 
printf "EXECUTING job: ${KMEANS_PATH}\n\n"
mahout/bin/mahout kmeans -i filtered_tfidf/l2_${NGRAM}gram/tfidf-vectors/ -c clusters_1 -o clustering_output/${KMEANS_PATH} -cd $CD -x $X -k 10 -cl -ow | tee clusterdump_output/logs/${KMEANS_PATH}.log

# Dump normal kmeans output
printf "DUMPING job: ${KMEANS_PATH}\n\n"
mahout/bin/mahout clusterdump -d filtered_tfidf/l2_${NGRAM}gram/dictionary.file-0 -dt sequencefile -i clustering_output/$KMEANS_PATH/clusters-2-final -n 10 -b 100 -o clusterdump_output/${KMEANS_PATH}.out | tee -a clusterdump_output/logs/$KMEANS_PATH.log

# Run Fuzzy kmeans
printf "EXECUTING job: ${FKMEANS_PATH}\n\n"
mahout/bin/mahout fkmeans -i filtered_tfidf/l2_${NGRAM}gram/tfidf-vectors/ -c clusters_1 -o clustering_output/${FKMEANS_PATH} -cd $CD -x $X -k 10 -cl -ow -m $M | tee clusterdump_output/logs/${FKMEANS_PATH}.log

# Dump Fuzzy means output
printf "DUMPING job: ${FKMEANS_PATH} {cd: $CD, x: $X, ngram: $NGRAM, m: $M}\n\n"
mahout/bin/mahout clusterdump -d filtered_tfidf/l2_${NGRAM}gram/dictionary.file-0 -dt sequencefile -i clustering_output/${FKMEANS_PATH}/clusters-2-final -n 10 -b 100 -o clusterdump_output/${FKMEANS_PATH}.out | tee -a clusterdump_output/logs/$FKMEANS_PATH.log
