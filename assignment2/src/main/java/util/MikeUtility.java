package util;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * This class is used to do utility and testing work before shipping project
 * This class should not be included with the production work
 * @author Michael
 *
 */
public class MikeUtility {
	public static void main (String[] args) throws IOException {
		
		BufferedReader reader = new BufferedReader(new FileReader(args[0]));
		
		HashMap<String, Map<String, Double>> jobMap = buildJobMap(reader);
		reader.close();
		
		//For each map
		for (Map<String, Double> thisMap: jobMap.values()) {
			
			//For each entry in this map
			for (Entry<String, Double> entry : thisMap.entrySet()) {
				
				System.out.println(entry.getKey().toString() + ", " + entry.getValue());
				
				
				
			}
			
			
		}
	}
	
	
	
	private static HashMap<String, Map<String, Double>> buildJobMap(BufferedReader reader) throws IOException {
		
		HashMap<String, Map<String, Double>> outputMap = new HashMap<String, Map<String, Double>>();
		
		//This loop builds each sub map for each profession
		while (reader.ready()) {
			
			String inputLine = reader.readLine();
			
			String[] splitLine = inputLine.split("\t");
				
			StringDoubleList list = new StringDoubleList();
			
			list.readFromString(inputLine.split("\t")[1].trim());
				
			outputMap.put(splitLine[0].trim(), list.getMap());
		}
		
		return outputMap;
	}
	
	
	
	
	/*
	 * General notes:
	 * Total people: 673988
	 * Total professions: 881459
	 * Max likelihood: 0.10629195458892586
	 * Min likelihood: 5.558965306384074E-5
	 */
	public static void buildMap() throws IOException {
		BufferedReader input = new BufferedReader(new FileReader(new File("profession/profession_train.txt")));
		HashMap<String, Integer> map = new HashMap<String, Integer>();
		
		int total = 0;
//		int lines = 0;
		String line = null;
		
		while (input.ready()) {
//			lines++;
			line = input.readLine();
			String[] group = line.split(":");
			line = group[group.length - 1].trim();
			for (String profession : line.split(",")) {
				profession = profession.trim();
				total++;
				if (map.containsKey(profession)) {
					map.put(profession, map.get(profession) + 1);
				}
				else {
					map.put(profession, 1);
				}
			}
			
		}
		
//		System.out.println(line);
//		System.out.println(lines);
//		System.out.println(total);
		
		
		input.close();
		
		try (FileWriter output = new FileWriter("profession/professionm_map.txt")){
			
			Set<Entry<String, Integer>> set = map.entrySet();
			double max = 0;
			double min = 1;
			
			int enumNumber = 0;
			
			for (Entry<String, Integer> entry : set) {
				double prob = (double)entry.getValue()/(double)total;
				max = prob > max ? prob : max;
				min = prob < min ? prob : min;
				
				output.write(String.format("PROFESSION_%03d(\"%s\", %f),\n", enumNumber, entry.getKey(), prob));
				enumNumber++;
			}
			
//			System.out.println(max);
//			System.out.println(min);
//			System.out.println(total);
			
			
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

}
