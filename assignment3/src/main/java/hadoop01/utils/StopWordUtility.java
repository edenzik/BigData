package hadoop01.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;



/**
 * This class is used to compute statistics on the data and create stop word lists based
 * on the frequency of words.
 * @author Michael
 *
 */
public class StopWordUtility {
	public static void main (String[] args) {

		topBottom(.0002, .01);

	}
	
	public static void printStats() {
		
		int count = 0;
		
		try (BufferedReader reader = new BufferedReader (new FileReader("pa3_freq.txt"))) {

			
			while (reader.ready()) {
				String line = reader.readLine();
				
				if (count % 100 == 0) {
					System.out.println(line);
				}
				
				count++;
			}
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		System.out.println(count);
		
	}
	
	public static void topBottom(double highFilterRatio, double lowFilterRatio) {
		
		final int totalCount = 397268;
		int count = 0;
		int outCount = 0;
		
		try (BufferedReader reader = new BufferedReader (new FileReader("pa3_freq.txt"));
				FileWriter writer = new FileWriter("stopList.txt")) {

			while (reader.ready()) {
				count++;
				String line = reader.readLine();
				
				if (count < totalCount*highFilterRatio || count > totalCount*(1-lowFilterRatio)) {
					outCount++;
					writer.write(line.split(" : ")[1] + "\n");
					System.out.println(line.split(" : ")[1]);
				}
				
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		System.out.println(outCount);
	}

	public static void wordCount() {
		
		HashMap<String, Integer> wordCountMap = new HashMap<String, Integer>(10000);

		try (BufferedReader reader = new BufferedReader (new FileReader("input_pa3_random.txt"))) {

			while (reader.ready()) {
				String nextLine = reader.readLine().toLowerCase();
				nextLine = nextLine.replaceAll("[^a-z ]", " ");

				for (String word : nextLine.split(" ")) {
					if (!word.equals(" ") && !word.equals("")) {
						if (wordCountMap.containsKey(word)) {

							wordCountMap.put(word, wordCountMap.get(word) + 1);

						} else {

							wordCountMap.put(word, 1);

						}
					}
				}

			}			

		} catch ( IOException e ) {
			e.printStackTrace();
		}

		System.out.println(wordCountMap.size());
		MikeUtility ut = new MikeUtility();
		ValueComparator bvc =  ut.new ValueComparator(wordCountMap);
        TreeMap<String,Integer> sortedMap = new TreeMap<String,Integer>(bvc);
        
		sortedMap.putAll(wordCountMap);

		System.out.println(sortedMap.size());

		try (FileWriter writer = new FileWriter("pa3_freq.txt")) {

			for (Entry<String, Integer> e : sortedMap.entrySet()) {
				writer.write(e.getValue() + " : " + e.getKey() + "\n");
			}


		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	class ValueComparator implements Comparator<String> {

		Map<String, Integer> base;
		public ValueComparator(Map<String, Integer> base) {
			this.base = base;
		}

		// Note: this comparator imposes orderings that are inconsistent with equals.    
		public int compare(String a, String b) {
			if (base.get(a) >= base.get(b)) {
				return -1;
			} else {
				return 1;
			} // returning 0 would merge keys
		}
	}

}	//End of Class
