/**
 * 
 */
package hadoop01.utils;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.AbstractMap.SimpleEntry;
import java.util.HashMap;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

/**
 * @author edenzik
 *
 */
public class readKmeans {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		try {
			String filename = args[0];
			System.out.println("Processing Kmeans File..." + filename);
			HashMap<String, SimpleEntry<Integer, Double>> output = reader(new BufferedReader(new FileReader(args[0])));
			System.out.println(output.get("money"));
		}catch (ArrayIndexOutOfBoundsException e) {
			System.out.println("No filename given.");
		}catch (FileNotFoundException e) {
			System.out.println("File not found.");
		}

	}
	
	private static HashMap<String, SimpleEntry<Integer, Double>> reader(BufferedReader br){
		HashMap<String, SimpleEntry<Integer, Double>> output = new HashMap<String, SimpleEntry<Integer, Double>>();
		StringBuilder sb = new StringBuilder();
		String line;
		try {
			while ((line = br.readLine()) != null){
				sb.append(line);
				sb.append(System.lineSeparator());
			}
			String clusterPattern = ":\\w+-\\d+";
			String[] clusters = sb.toString().split(clusterPattern);
			int currentCluster = 1;
			for (String cluster :clusters){
				String linePattern = "\\s+(\\w+)\\s+=>\\s*(\\d.\\d+)";
				Pattern r = Pattern.compile(linePattern);
				Matcher m = r.matcher(cluster);
				while (m.find()){
					//System.out.println(m.group(1));
					output.put(m.group(1), new SimpleEntry<Integer, Double>(Integer.valueOf(currentCluster), Double.parseDouble(m.group(2))));
					
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
