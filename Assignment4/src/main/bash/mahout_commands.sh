# Here are the actual commands we used to get the item similarities

mahout itemsimilarity --input <input> --output <output>  --similarityClassname SIMILARITY_LOGLIKELIHOOD -m 10 -tr 0 --tempDir <temp directory>

mahout itemsimilarity --input <input> --output <output>  --similarityClassname SIMILARITY_CITY_BLOCK -m 10 -tr 0 --tempDir <temp directory>

# Where <input> is our parsed/pre-processed inpu, and output is our output that
# Has not yet been formatted for final submission (that formatting is taken care
# of in hadoop01.utils.FormatFinalOutput.java
