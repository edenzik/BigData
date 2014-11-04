package hadoop01.utils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

public class ClusterPointMapper {
	
	private final static int NgramNumber = 1;

	/**
	 * Prints output file from the cluster dumps and original input
	 * @param args 0: kmeans dump, 1: fuzzy dump, 2: filtered review text
	 */
	public static void main(String[] args) {

		//			Map kmeansMap = TODO: MAP GENERATING CODE HERE
//					Map fuzzyMap = TODO: MAP GENERATING CODE HERE

		try {
			BufferedReader reader = new BufferedReader(new FileReader(args[2]));
			BufferedWriter kwriter = new BufferedWriter(new FileWriter("kmeans_output.txt"));
			BufferedWriter fwriter = new BufferedWriter(new FileWriter("fkmeans_output.txt"));
			
			//Match token against map
			HashMap<String, List<SimpleEntry<Integer, Double>>> kclusterMap = 
					readKmeans.readKMeansFile(args[0]);
			HashMap<String, List<SimpleEntry<Integer, Double>>> fclusterMap = 
					readKmeans.readKMeansFile(args[1]);;
			
			int linesRead = 0;
					
			System.out.println("Completed building maps, reading input");
			
			while (reader.ready()) {
				//Structures for matches to clusters
				HashMap<Integer, Integer> kmeansMatch = new HashMap<Integer, Integer>();
				HashMap<Integer, Double> fuzzyMatch = new HashMap<Integer, Double>();
				
				//Populate initial maps
				for (int i = 1; i < 10; i++) {
					kmeansMatch.put(i, 0);
					fuzzyMatch.put(i, 0.0);
				}
				
				//Read the line and split into tokens
				String line = reader.readLine();
				String[] tokens = line.split(" ");
				
				//Print when lines read reaches a big interval
				if (linesRead++ % 10000 == 0) {
					System.out.println("Read in " + linesRead + " lines.");
				}
				
				
				//For each token (feature) in the review
				for (int i = 0; i <= tokens.length - NgramNumber; i++) {
					
					//Normalize token by assembling Ngram
					String token = tokens[i];
					for (int x = 1; x < NgramNumber; x++) {
						token = token.concat(" " + tokens[i + x]);			
					}
					
					//Match token against maps
					List<SimpleEntry<Integer, Double>> kmatches = kclusterMap.get(token);
					List<SimpleEntry<Integer, Double>> fmatches = fclusterMap.get(token);
					

					if (kmatches != null) {
						//Update hashMaps with matched data
						//Update kmeansMatch
						for (SimpleEntry<Integer, Double> s : kmatches) {
							if (kmeansMatch.containsKey(s.getKey())) {
								kmeansMatch.put(s.getKey(),
										kmeansMatch.get(s.getKey()) + 1);
							} else {
								kmeansMatch.put(s.getKey(), 1);
							}
						}
					}
					
					if (fmatches != null) {
						//Update fuzzyMatch
						for (SimpleEntry<Integer, Double> s : fmatches) {
							if (fuzzyMatch.containsKey(s.getKey())) {
								fuzzyMatch.put(
										s.getKey(),
										fuzzyMatch.get(s.getKey())
												+ s.getValue());
							} else {
								fuzzyMatch.put(s.getKey(), s.getValue());
							}
						}
					}
					
					
					
					
					
				}	//End of this record
				
				//Print best match for kmeans
				int bestCluster = 1;
				for (Entry<Integer, Integer> e : kmeansMatch.entrySet()) {
					if (e.getValue() > kmeansMatch.get(bestCluster)) {
						//Found a new best match
						bestCluster = e.getKey();
					}					
				}
				kwriter.write(String.valueOf(bestCluster) + "\n");
				

				
				
				//Print all matches in sorted order for fuzzy					
				List<Entry<Integer, Double>> fuzzyMatchList = 
						new ArrayList<Entry<Integer, Double>>();
				for (Entry<Integer, Double> e : fuzzyMatch.entrySet()) {
					fuzzyMatchList.add(e);
				}
				
				
				Collections.sort(fuzzyMatchList, new ValueComparator());
				
				//Concatenate the output line
				String output = "";
				for (Entry<Integer, Double> e : fuzzyMatchList) {
					output = output.concat(e.getKey() + ":" + e.getValue());
					output = output.concat(", ");
				}
				output = output.substring(0, output.length() - 2);
				
				fwriter.write(output + "\n");


			}

			reader.close(); 
			kwriter.flush(); kwriter.close();
			fwriter.flush(); fwriter.close();

		} catch (IOException e) {
			e.printStackTrace();
		}

	}


}
