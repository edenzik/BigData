#!/bin/bash

# Runs multiple k-means and fuzzy k-means mahout clustering jobs
# to test different parameters. This uses kmeans.sh

# Test fuzziness
./kmeans.sh .05 10 1 1.0
./kmeans.sh .05 10 1 1.5
./kmeans.sh .05 10 1 2.0

# Test n-gram
./kmeans.sh .05 10 1 1.5
./kmeans.sh .05 10 2 1.5
./kmeans.sh .05 10 3 1.5
./kmeans.sh .05 10 4 1.0

