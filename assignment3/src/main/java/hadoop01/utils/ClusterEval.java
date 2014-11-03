package hadoop01.utils;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.mahout.common.distance.DistanceMeasure;
import org.apache.mahout.common.distance.EuclideanDistanceMeasure;
import org.apache.mahout.math.NamedVector;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.Vector.Element;
import org.apache.mahout.math.VectorWritable;
import org.apache.mahout.math.stats.OnlineSummarizer;
import org.apache.mahout.clustering.ClusteringUtils;

public class ClusterEval {

	/**
	 * Evaluates clusters using Mahout's ClusterUtils
	 * @param args 0: Path to dictionary file, 1: Path to vector sequence file
	 * @throws NumberFormatException
	 * @throws IOException
	 */
	public static void main(String[] args) throws NumberFormatException, IOException {

		//Configuration Boiler Plate
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		String vectorsPathString = args[1];
		String dictionaryPathString = args[0];
		Path vectorsPath = new Path(vectorsPathString);
		Path dictionaryPath = new Path(dictionaryPathString);
		

		
		//Read in dictionary
		SequenceFile.Reader read = new SequenceFile.Reader(fs, dictionaryPath, conf);
		IntWritable dicKey = new IntWritable();
		Text text = new Text();
		HashMap<Integer, String> dictionaryMap = new HashMap<Integer, String>();
		while (read.next(text, dicKey)) {
			dictionaryMap.put(Integer.parseInt(dicKey.toString()), text.toString());
		}
		read.close();

		//Read in vectors
		SequenceFile.Reader reader = new SequenceFile.Reader(fs, vectorsPath, conf);
		LongWritable key = new LongWritable();
		VectorWritable value = new VectorWritable();
		while (reader.next(key, value)) {
			NamedVector namedVector = (NamedVector)value.get();
			RandomAccessSparseVector vect = (RandomAccessSparseVector)namedVector.getDelegate();

			for( Element  e : vect ){
				System.out.println("Token: "+dictionaryMap.get(e.index())+", TF-IDF weight: "+e.get()) ;
			}
		}
		reader.close();

		List<Vector> centroids = null;
		List<Vector> datapoints = null;
		DistanceMeasure distMeasure = new EuclideanDistanceMeasure();
		List<OnlineSummarizer> clusterDistanceSummaries = ClusteringUtils.summarizeClusterDistances(
				datapoints, centroids, distMeasure);


		double index = 
				ClusteringUtils.dunnIndex(centroids, distMeasure, clusterDistanceSummaries);

		System.out.println("Dunn Clustering Evaluation Index: " + index);

	}

}
