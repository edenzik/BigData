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

	public HashMap<String, SimpleEntry<Integer, Double>> readKMeansFile(String fileLocation){
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
	
	private static HashMap<String, SimpleEntry<Integer, Double>> parseKMeansFile(BufferedReader br){
		HashMap<String, SimpleEntry<Integer, Double>> output = new HashMap<String, SimpleEntry<Integer, Double>>();
		StringBuilder sb = new StringBuilder();
		String line;
		try {
			while ((line = br.readLine()) != null){
				sb.append(line);
				sb.append(System.lineSeparator());
			}
			String clusterPattern = ":\\w+-\\d+";			//Split each cluster
			String[] clusters = sb.toString().split(clusterPattern);
			int currentCluster = 1;
			for (String cluster :clusters){
				String linePattern = "\\s+(\\w+)\\s+=>\\s*(\\d.\\d+)";		//Split 
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
