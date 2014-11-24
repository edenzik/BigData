package hadoop01.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Evaluates the addition of the reverse mappings of the similarity measure
 * to ensure that nothing went astray there
 * 
 * @author kahliloppenheimer
 *
 */
public class EvaluateReverseMapping {

	/**
	 * Evaluates a mapping and reverse mapping to make sure they were done correctly
	 * 
	 * Takes two arguments:
	 * 1) File with original product recommendations
	 * 2) File with product recommendations with reverse mappings
	 */
	public static void main(String[] args) throws IOException {
		if(args.length != 2) {
			throw new IllegalArgumentException("Must supply two arguments!");
		}
		File originalRec = new File(args[0]);
		File reversedRec = new File(args[1]);
		if(!originalRec.canRead() || !reversedRec.canRead()) {
			throw new IOException("Can not read files!");
		}

		System.out.println("Building original map...");
		Map<Integer, Integer> originalMap = buildMap(originalRec);
		System.out.println("Building reversed map...");
		Map<Integer, Integer> reversedMap = buildMap(reversedRec);

		System.out.println("Evaluating Cardinalities");
		if(!cardinalitiesMatch(originalMap, reversedMap)) {
			System.err.println("Cardinality mismatch!");
		}
	}

	/**
	 * Returns whether or not the cardinalities of set of keys + values for each
	 * map matches
	 * 
	 * @param originalMap
	 * @param reversedMap
	 * @return
	 */
	private static boolean cardinalitiesMatch(
			Map<Integer, Integer> originalMap, Map<Integer, Integer> reversedMap) {
		// Set to contain the union of the set of keys and values (duplicates
		// will be removed). Notably this set has to be seperate from what is
		// returned by Map::keySet(), because that map is read-only.
		Set<Integer> originalAgg = new HashSet<Integer>();
		originalAgg.addAll(originalMap.keySet());
		originalAgg.addAll(originalMap.values());
		
		Set<Integer> reversedAgg = new HashSet<Integer>();
		reversedAgg.addAll(reversedMap.keySet());
		reversedAgg.addAll(reversedMap.values());
		
		System.out.println("Original cardinality = " + originalAgg.size());
		System.out.println("Reversed cardinality = " + reversedAgg.size());
		
		System.out.println("reversed - original = " + (reversedAgg.size() - originalAgg.size()));
		
		return true;
	}

	/**
	 * Builds a mapping from item_id to it's recommendations given a CSV file
	 * of the following input format:
	 * 
	 * item_id		reccomended_item_id		similarity_measure
	 * 
	 * @param f
	 * @return
	 */
	private static Map<Integer, Integer> buildMap(File f) {
		Map<Integer, Integer> map = new HashMap<Integer, Integer>();
		BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader(f));
			String nextLine = null;
			while((nextLine = br.readLine()) != null) {
				String[] splitLine = nextLine.split("\\s+");
				myAssert(splitLine.length == 3);
				map.put(Integer.parseInt(splitLine[0].trim()),
						Integer.parseInt(splitLine[1].trim()));

			}
		} catch(IOException e) {
			e.printStackTrace();
		} finally {
			try {
				br.close();
			} catch(IOException e) {
				e.printStackTrace();
			}
		}
		return map;
	}

	/**
	 * Throws an error if the passed boolean is false
	 * 
	 * @param b
	 * @return
	 */
	private static boolean myAssert(boolean b) {
		if(!b) {
			throw new Error("Assertion failed!");
		}
		return true;
	}

}
