package hadoop01.utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.mahout.clustering.ClusteringUtils;
import org.apache.mahout.common.distance.DistanceMeasure;
import org.apache.mahout.common.distance.EuclideanDistanceMeasure;
import org.apache.mahout.math.NamedVector;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.apache.mahout.math.stats.OnlineSummarizer;
/**
 * Class for evaluating clusters, currently only Dunn evaluation is performed
 * 
 * Most of this code comes from various sources on the internet found through google searches
 * None of this code is my own, original work
 * @author Michael Partridge
 *
 */
public class ClusterEval {

	/**
	 * Evaluates clusters using Mahout's ClusterUtils
	 * @param args 0: Path to dictionary file, 1: Path to vector sequence file, 2: Path to centroids
	 * @throws NumberFormatException
	 * @throws IOException
	 */
	public static void main(String[] args) throws NumberFormatException, IOException {

		//Configuration Boiler Plate
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		String vectorsPathString = args[1];
		String dictionaryPathString = args[0];
		String centroidsPathString = args[2];
		Path vectorsPath = new Path(vectorsPathString);
		Path dictionaryPath = new Path(dictionaryPathString);
		Path centroidsPath = new Path(centroidsPathString);
		
		List<Vector> centroids = new ArrayList<Vector>();
		List<Vector> datapoints = new ArrayList<Vector>();

		
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
		Text key = new Text();
		VectorWritable value = new VectorWritable();
		while (reader.next(key, value)) {
			//Not sure whether vectors are NamedVectors or RandomAccessSparseVectors
			NamedVector namedVector = (NamedVector)value.get();
//			RandomAccessSparseVector vect = (RandomAccessSparseVector)namedVector.getDelegate();
			datapoints.add(namedVector);
		}
		reader.close();
		
		//Read in centroids
		reader = new SequenceFile.Reader(fs, centroidsPath, conf);
		Text ckey = new Text();
		VectorWritable cvalue = new VectorWritable();
		while (reader.next(ckey, cvalue)) {
			NamedVector namedVector = (NamedVector)cvalue.get();
//			RandomAccessSparseVector vect = (RandomAccessSparseVector)namedVector.getDelegate();
			datapoints.add(namedVector);
		}
		reader.close();
		

		//Input data is built, run distance measure
		
		DistanceMeasure distMeasure = new EuclideanDistanceMeasure();
		List<OnlineSummarizer> clusterDistanceSummaries = ClusteringUtils.summarizeClusterDistances(
				datapoints, centroids, distMeasure);

		//Report results
		double index = 
				ClusteringUtils.dunnIndex(centroids, distMeasure, clusterDistanceSummaries);

		System.out.println("Dunn Clustering Evaluation Index: " + index);

	}

}
