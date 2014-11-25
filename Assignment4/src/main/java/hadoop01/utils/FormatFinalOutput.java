/**
 * 
 */
package hadoop01.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

/**
 * @author kahliloppenheimer
 *
 */
public class FormatFinalOutput {

	// Used to preserve ordering of insertion into set
	// for the assigned items (since our output must be
	// in the same order)

	/**
	 * Takes in input CSV file of the following format:
	 * 
	 * item_id		reccomended_item_id			similarity
	 * 
	 * and outputs the final following format
	 * 
	 * item_id,reccommended_item_id1,reccomended_item_id2, ... , reccomended_itemid10
	 * 
	 * @param args
	 */
	public static void main(String[] args) throws IOException {
		System.out.println("Starting");
		// Validate args length
		if(args.length != 4) {
			throw new IllegalArgumentException("Must supply input_recommendations, item_id_dictionary, assigned items, and output file");
		}

		// Validate files
		File inputRecommendationsFile = new File(args[0]);
		if(!inputRecommendationsFile.canRead()) {
			throw new IOException("Cannot read: " + inputRecommendationsFile);
		}
		File itemIdDictFile = new File(args[1]);
		if(!itemIdDictFile.canRead()) {
			throw new IOException("Cannot read: " + itemIdDictFile);
		}
		File assignedItemFile = new File(args[2]);
		if(!assignedItemFile.canRead()) {
			throw new IOException("Cannot read: " + assignedItemFile);
		}
		File outputFile = new File(args[3]);
		if(outputFile.exists() && !outputFile.canWrite()) {
			throw new IOException("Cannot write to: " + outputFile);
		}

		// Set of assigned item ids in order presented in file
		System.out.println("Reading in assigned items...");
		Set<String> origAssignedIIDSet = getAssignedItemSet(assignedItemFile);
		//System.out.println(origAssignedIIDSet);
		System.out.println("Converting assigned items to their relative ids...");
		Set<Integer> relAssignedIIDSet = getRelativeIIDSet(origAssignedIIDSet, itemIdDictFile);
		//System.out.println(relAssignedIIDSet);
		if(origAssignedIIDSet.size() != relAssignedIIDSet.size()) {
			throw new Error("Some assigned items not in dictionary");
		}

		// Filter out non-relevant recommendations
		System.out.println("Filtering assigned item recommendations");
		List<int[]> filteredRecs = filterItemRecs(inputRecommendationsFile, relAssignedIIDSet);

		System.out.println("Filtered recs = " + filteredRecs);

		// Map of relevant item ids to original ids
		System.out.println("Building relevant itemID dictionary...");
		Map<Integer, String> itemIdDict = getItemIdDict(itemIdDictFile, filteredRecs);
		//System.out.println(itemIdDict);

		// Map of assigned original ids to their recommendations (capped at 10 each)
		System.out.println("Reformatting assigned item recommendations...");
		Map<String, List<String>> formattedItemRecs =
				formatRecommendations(filteredRecs, itemIdDict, relAssignedIIDSet);

		// Reorders map to be in the same order as the input
		System.out.println("Reordering assigned item recommendations...");
		Map<String, List<String>> reorderedItemRecs = reorderMap(formattedItemRecs, origAssignedIIDSet);
		// Prints the map in the assignment's specified format
		System.out.println("Printing out assigned item recommendations...");
		printMap(reorderedItemRecs, outputFile);
	}

	/**
	 * Go through the input recommendations and return a list of strings of all
	 * of the relevant recommendations (i.e. only those whose key are assigned)
	 * @param inputRecs
	 * @param relAssignedIIDSet
	 * @return
	 * @throws IOException
	 */
	private static List<int[]> filterItemRecs(File inputRecs,
			Set<Integer> relAssignedIIDSet) throws IOException {
		List<int[]> filteredRecs = new ArrayList<int[]>();
		BufferedReader br = new BufferedReader(new FileReader(inputRecs));
		String nextLine = null;
		while((nextLine = br.readLine()) != null) {
			String[] splitLine = nextLine.split("\\s+");
			if(splitLine.length == 3) {
				Integer IID = Integer.parseInt(splitLine[0].trim());
				Integer recID = Integer.parseInt(splitLine[1].trim());
				if(relAssignedIIDSet.contains(IID)) {
					filteredRecs.add(new int[] {IID, recID});
				}
			}
		}
		br.close();
		return filteredRecs;
	}

	/**
	 * Prints a given map according the the assignment's specifications.
	 * 
	 * @param map
	 * @param outputFile 
	 * @throws FileNotFoundException 
	 * @throws UnsupportedEncodingException 
	 */
	private static void printMap(
			Map<String, List<String>> map, File outputFile) throws FileNotFoundException, UnsupportedEncodingException {
		PrintWriter out = new PrintWriter(outputFile, "UTF-8");
		System.out.println(map);
		for(String s: map.keySet()) {
			StringBuilder nextLine = new StringBuilder();
			nextLine.append(s);
			System.out.println("s = " + s + "\tmap.get(s) = " + map.get(s));
			List<String> recs = map.get(s);
			if(recs == null) {
				recs = getRandomTen(map.keySet());
			}
			for(String t : recs) {
				nextLine.append("," + t);
			}
			out.println(nextLine);
		}
		out.close();
	}

	private static List<String> getRandomTen(Set<String> set) {
		Random rand = new Random();
		List<String> old = new ArrayList<String>(set);
		List<String> newList = new ArrayList<String>();
		for(int i = 0; i < 10; ++i) {
			int idx = rand.nextInt(old.size());
			newList.add(old.get(idx));
		}
		return newList;
	}

	/**
	 * Returns a mapping of item_id -> list of recommended item_ids
	 * with no more than 10 recommended item_ids per items. Notably, the
	 * filteredRecs list passed to this method only contains the relevant
	 * item ids (i.e. the assigned ones)
	 * 
	 * @param filteredRecs
	 * @param itemIdDict
	 * @param assignedItemSet
	 * @return
	 * @throws IOException
	 */
	private static Map<String, List<String>> formatRecommendations(
			List<int[]> filteredRecs, Map<Integer, String> itemIdDict, Set<Integer> relAssignedIIDSet) throws IOException {
		// Final list of item recommendations
		Map<String, List<String>> assignedRecs = new HashMap<String, List<String>>();
		for(int[] arr: filteredRecs) {
			// Original item ID
			String origIID = itemIdDict.get(arr[0]);
			// Original recommended item ID
			String recIID = itemIdDict.get(arr[1]);
			// Only add to list if there are less than 10 recs present
			if(assignedRecs.containsKey(origIID) && assignedRecs.get(origIID).size() < 10) {
				assignedRecs.get(origIID).add(recIID);
			} else {
				List<String> itemRecs = new LinkedList<String>();
				itemRecs.add(recIID);
				assignedRecs.put(origIID, itemRecs);
			}
		}
		return assignedRecs;
	}

	/**
	 * Takes a map and an ordered set and returns a map whose entries are ordered
	 * exactly the same as the list
	 * 
	 * @param origMap
	 * @param s
	 * @return
	 */
	private static Map<String, List<String>> reorderMap(
			Map<String, List<String>> unorderedMap, Set<String> origAssignedIIDSet) {
		LinkedHashMap<String, List<String>> reorderedMap = new LinkedHashMap<String, List<String>>();
		for(String s: origAssignedIIDSet) {
			reorderedMap.put(s, unorderedMap.get(s));
		}
		System.out.println(reorderedMap);
		return reorderedMap;
	}

	/**
	 * Returns a mapping of our relative item_ids to the originals item_ids. This method
	 * smartly will only create mappings for relevant ids (i.e. those that have an assigned
	 * item id as the first entry)
	 * 
	 * @param itemIdDictFile
	 * @param filteredRecs 
	 * @return
	 * @throws IOException 
	 */
	private static Map<Integer, String> getItemIdDict(File itemIdDictFile, List<int[]> filteredRecs) throws IOException {
		// Create a set of all relevantIIDs for convenience
		Set<Integer> relevantIIDs = new HashSet<Integer>();
		for(int[] arr: filteredRecs) {
			for(int i: arr) {
				relevantIIDs.add(i);
			}
		}
		System.out.println(relevantIIDs);

		Map<Integer, String> itemIdDict = new HashMap<Integer, String>();
		BufferedReader br = new BufferedReader(new FileReader(itemIdDictFile));
		String nextLine = null;
		while((nextLine = br.readLine()) != null) {
			String[] splitLine = nextLine.split(",");
			Integer keyRelIID = Integer.parseInt(splitLine[1].trim());
			if(relevantIIDs.contains(keyRelIID)) {
				String keyOrigId = splitLine[0].trim();
				itemIdDict.put(keyRelIID, keyOrigId);
			}
		}
		br.close();

		return itemIdDict;
	}

	/**
	 * Reads in the input file of items to get recomendations on and returns
	 * a LinkedHashSet of the items, such that the ordering in the file
	 * is preserved.
	 * 
	 * @param inputFile
	 * @return 
	 * @throws IOException
	 */
	private static Set<String> getAssignedItemSet(File inputFile) throws IOException {
		Set<String> assignedItems = new LinkedHashSet<String>();
		BufferedReader br = new BufferedReader(new FileReader(inputFile));
		String nextLine = null;
		while((nextLine = br.readLine()) != null) {
			assignedItems.add(nextLine.trim());
		}
		br.close();
		return assignedItems;
	}

	/**
	 * Returns the mappings of all relative id -> original id that are relevant
	 * (i.e. that belong to an assigned item or one if its recommendations)
	 * 
	 * @param origAssignedIIDSet2
	 * @param itemIdDictFile
	 * @return
	 * @throws IOException
	 */
	private static Set<Integer> getRelativeIIDSet(Set<String> origAssignedIIDSet, File itemIdDictFile) throws IOException {
		Set<Integer> relAssignedIIDSet = new HashSet<Integer>();
		BufferedReader br = new BufferedReader(new FileReader(itemIdDictFile));
		String nextLine = null;
		// Go through itemIdDict and create mappings from all assigned item ids to their relative ids
		while((nextLine = br.readLine()) != null) {
			String[] splitLine = nextLine.split(",");
			if(splitLine.length == 2) {
				String origIID = splitLine[0].trim();
				if(origAssignedIIDSet.contains(origIID)) {
					Integer relIID = Integer.parseInt(splitLine[1].trim());
					relAssignedIIDSet.add(relIID);
				}
			}
		}
		br.close();
		return relAssignedIIDSet;
	}

}
