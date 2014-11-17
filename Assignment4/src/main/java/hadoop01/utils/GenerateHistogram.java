package hadoop01.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public class GenerateHistogram {
	// input CSV file with format
	// user_id,item_id,review_score
	public static File INPUT_CSV_FILE;
	// map of item_id -> # reviews
	public static Map<Integer, Integer> item_to_reviews;
	// position of item_id in each CSV line (i.e. second value is index 1)
	public static final int ITEM_ID_IDX= 1;
	// output stream for histogram
	public static PrintStream output;

	// input: input_file, output_file (or none for stdout)
	// output: map of num_reviews -> num_items
	public static void main(String[] args) throws IOException {
		// Ensure only 1 arg passed
		if(args.length < 1 || args.length > 2) {
			throw new IllegalArgumentException("args: input file, outputfile (optional)");
		}
		
		// Verify input file
		INPUT_CSV_FILE = new File(args[0]);
		// Ensure file is readable
		if(!INPUT_CSV_FILE.canRead()) {
			throw new IOException("Cannot read: " + INPUT_CSV_FILE);
		}
		
		// Verify printstream
		if(args.length == 2) {
			output = new PrintStream(args[1]);
		} else {
			output = System.out;
		}
		item_to_reviews = new HashMap<Integer, Integer>();
		populateItemToReviews();
		TreeMap<Integer, Set<Integer>> reviewFrequencies =
				new TreeMap<Integer, Set<Integer>>(reverseMap(item_to_reviews));
		// Print histogram results
		for(Integer i: reviewFrequencies.keySet()) {
			output.println(i + "\t" + reviewFrequencies.get(i).size());
		}
		
	}

	// Populates a map of item_id -> # reviews
	private static void populateItemToReviews() throws IOException {
		BufferedReader br = new BufferedReader(new FileReader(INPUT_CSV_FILE));
		String nextLine = null;
		while((nextLine = br.readLine()) != null) {
			int item_id = Integer.parseInt(nextLine.split(",")[ITEM_ID_IDX]);
			if(!item_to_reviews.containsKey(item_id)) {
				item_to_reviews.put(item_id, 1);
			} else {
				item_to_reviews.put(item_id, item_to_reviews.get(item_id) + 1);
			}
		}
		br.close();
	}
	
	// Returns the reverse map of a given map, i.e. value -> {keys}
	private static <K> Map<Integer, Set<K>> reverseMap(Map<K, Integer> map) {
		Map<Integer, Set<K>> reverseMap = new HashMap<Integer, Set<K>>();
		for(K key: map.keySet()) {
			Integer numReviews = map.get(key);
			Set<K> item_ids = null;
			if(!reverseMap.containsKey(numReviews)) {
				item_ids = new HashSet<K>();
			} else {
				item_ids = reverseMap.get(numReviews);
			}
			item_ids.add(key);
			reverseMap.put(numReviews, item_ids);
		}
		return reverseMap;
	}
}
