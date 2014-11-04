package hadoop01.utils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

public class ClusterPointMapper {
	
	private final static int NgramNumber = 1;

	public static void main(String[] args) {

		//			Map kmeansMap = TODO: MAP GENERATING CODE HERE
//					Map fuzzyMap = TODO: MAP GENERATING CODE HERE

		try {
			BufferedReader reader = new BufferedReader(new FileReader(args[1]));
			BufferedWriter kwriter = new BufferedWriter(new FileWriter("kmeans_output.txt"));
			BufferedWriter fwriter = new BufferedWriter(new FileWriter("fkmeans_output.txt"));
			while (reader.ready()) {
				//Structures for matches to clusters
				HashMap<Integer, Integer> kmeansMatch = new HashMap<Integer, Integer>();
				HashMap<Integer, Double> fuzzyMatch = new HashMap<Integer, Double>();
				
				String line = reader.readLine();
				String[] tokens = line.split(" ");
				
				for (int i = 0; i <= tokens.length - NgramNumber; i++) {
					
					//Make token by assembling Ngram
					String token = tokens[i];
					for (int x = 1; x < NgramNumber; x++) {
						token = token.concat(" " + tokens[i + x]);
					}
					
					//Match token against map
					//TODO: Match tokens against both maps
					
					
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
				
				


			}

			reader.close(); 
			kwriter.flush(); kwriter.close();
			fwriter.flush(); fwriter.close();

		} catch (IOException e) {
			e.printStackTrace();
		}

	}


}
