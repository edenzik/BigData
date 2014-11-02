package hadoop01.utils;

import java.util.List;

import org.apache.mahout.common.distance.DistanceMeasure;
import org.apache.mahout.common.distance.EuclideanDistanceMeasure;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.stats.OnlineSummarizer;
import org.apache.mahout.clustering.ClusteringUtils;

public class ClusterEval {

	public static void main(String[] args) {
		
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
