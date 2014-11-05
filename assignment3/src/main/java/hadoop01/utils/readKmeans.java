package hadoop01.utils;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author edenzik
 *
 */
public class readKmeans {
	
	// Main method used for unit testing only
	public static void main(String[] args) {
		String FILE_LOCATION = "clusterdump.out";
		Map<String, List<SimpleEntry<Integer, Double>>> m = readKMeansFile(FILE_LOCATION);
		int numVals = 0;
		// Test to make sure that there are 100 cluster values
		for(List<SimpleEntry<Integer, Double>> l: m.values()) {
			numVals += l.size();
		}
		assert(numVals == 100);
		System.out.println(m);
	}

	public static Map<String, List<SimpleEntry<Integer, Double>>> readKMeansFile(String fileLocation){
		try {
			//System.out.println("Processing Kmeans File..." + filename);
			return parseKMeansFile(new BufferedReader(new FileReader(fileLocation)));
			//System.out.println(output.get("money"));
		}catch (ArrayIndexOutOfBoundsException e) {
			System.out.println("No filename given.");
		}catch (FileNotFoundException e) {
			System.out.println("File not found.");
		}
		return null;

	}
	
	private static Map<String, List<SimpleEntry<Integer, Double>>> parseKMeansFile(BufferedReader br){
		Map<String, List<SimpleEntry<Integer, Double>>> output = new HashMap<String, List<SimpleEntry<Integer, Double>>>();
		StringBuilder sb = new StringBuilder();
		String line;
		try {
			while ((line = br.readLine()) != null){
				sb.append(line);
				sb.append(System.lineSeparator());
			}
			String clusterPattern = ":\\w+-\\d+";			//Split each cluster
			String[] clusters = sb.toString().split(clusterPattern);
			System.out.println(clusters.length);
			System.out.println(clusters[0]);
			int currentCluster = 0;
			for (String cluster :clusters){
				String linePattern = "\\s+(\\w+\\s*\\w*)\\s+=>\\s*(\\d.\\d+)";		//Split 
				Pattern r = Pattern.compile(linePattern);
				Matcher m = r.matcher(cluster);
				while (m.find()){
					// ALWAYS TRIM YOUR STRINGS
					String next = m.group(1).trim();
					//System.out.println(next);
					if(!output.containsKey(next)){
						output.put(next, (new ArrayList<SimpleEntry<Integer, Double>>()));
					}

					(output.get(next)).add(new SimpleEntry<Integer, Double>(Integer.valueOf(currentCluster), Double.parseDouble(m.group(2).trim())));
					
				}
				currentCluster++;
			}
			br.close();
		} catch (IOException e) {
			System.out.println("Failed to read.");
		}
		return output;
	}

}
