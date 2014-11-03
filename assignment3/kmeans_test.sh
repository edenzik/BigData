#!/bin/bash

# Runs multiple k-means and fuzzy k-means mahout clustering jobs
# to test different parameters. This uses kmeans.sh

./kmeans.sh .005 10 1 1.01 Cosine

# Test distance measure
# ./kmeans.sh .05 10 1 1.1 Cosine
# ./kmeans.sh .05 10 1 1.1 Manhattan
# ./kmeans.sh .05 10 1 1.1 WeightedManhattan
# ./kmeans.sh .05 10 1 1.1 Chebyshev
# ./kmeans.sh .05 10 1 1.1 Mahalanobis
# ./kmeans.sh .05 10 1 1.1 Minkowski
# ./kmeans.sh .05 10 1 1.1 Euclidean
# ./kmeans.sh .05 10 1 1.1 Weighted
# ./kmeans.sh .05 10 1 1.1 WeightedEuclidean

# Test fuzziness
# ./kmeans.sh .05 10 1 1.1
# ./kmeans.sh .05 10 1 1.2
# ./kmeans.sh .05 10 1 1.3
# ./kmeans.sh .05 10 1 1.4

# Test n-gram
#./kmeans.sh .05 10 1 1.5
#./kmeans.sh .05 10 2 1.5
#./kmeans.sh .05 10 3 1.5
#./kmeans.sh .05 10 4 1.5

