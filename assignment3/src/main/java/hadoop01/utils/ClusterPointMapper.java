package hadoop01.utils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;

public class ClusterPointMapper {
	
	private final static int NgramNumber = 1;

	public static void main(String[] args) {

		//			Map centerMap = MAP GENERATING CODE HERE

		try {
			BufferedReader reader = new BufferedReader(new FileReader(args[1]));
			BufferedWriter writer = new BufferedWriter(new FileWriter("kmeans_output.txt"));
			while (reader.ready()) {
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
					Object matchInfo = 
					
					
				}
				
				


			}

			reader.close(); writer.flush(); writer.close();

		} catch (IOException e) {
			e.printStackTrace();
		}

	}

}
