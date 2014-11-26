import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;


public class OutputUtility {

	public static void main(String[] args) {

		if (args.length < 1) {

			System.out.println("Usage:");
			System.out.println("minreco <file> <min>: Prints all items with less than (min) "
					+ "recommendations from (file).");	
			System.out.println("avgreco <file>: Prints average, max, min recommendations "
					+ "from (file).");	
		} else if (args[0].equals("minreco")) {

			if (args.length != 3) {
				System.out.println("Invalid argument.");
				System.out.println("Usage:");
				System.out.println("minreco <file> <min>: Prints all items with less than (min) "
						+ "recommendations from (file).");	
			} else {

				printFewReviews(args[1], Integer.parseInt(args[2]));

			}
		} else if (args[0].equals("avgreco")) {

			if (args.length != 2) {
				System.out.println("Invalid argument.");
				System.out.println("Usage:");
				System.out.println("avgreco <file>: Prints average, max, min recommendations "
						+ "from (file).");	
			} else {

				printAverageReviews(args[1]);

			}
		}






	}


	/**
	 * Searches recommendation output for items with few recommendations and prints those items
	 * @param input File to analyze
	 * @param min Minimum number of reviews per item
	 */
	private static void printFewReviews(String input, int min) {


		Map<Integer, Integer> reviewCountMap = new HashMap<Integer, Integer>();

		System.out.println("Reading file in...");
		
		buildItemRecoMap(reviewCountMap, input);
		
		System.out.println("File read. Processing output...");

		printMapMin(reviewCountMap, min);

	}
	
	
	/**
	 * Searches recommendation output for items with few recommendations and prints those items
	 * @param input File to analyze
	 * @param min Minimum number of reviews per item
	 */
	private static void printAverageReviews(String input) {

		Map<Integer, Integer> reviewCountMap = new HashMap<Integer, Integer>();

		System.out.println("Reading file in...");
		
		buildItemRecoMap(reviewCountMap, input);
		
		System.out.println("File read. Processing output...");

		printMapAvg(reviewCountMap);

	}

	private static void printMapAvg(Map<Integer, Integer> reviewCountMap) {
		
		int sum = 0;
		int min = Integer.MAX_VALUE;
		int minItem = -1;
		int max = 0;
		int maxItem = -1;
		
		for (Integer itemNo : reviewCountMap.keySet()) {

			int value = reviewCountMap.get(itemNo);
			
			sum += value;
			if (value < min) {
				min = value;
				minItem = itemNo;
			}
			if (value > max) {
				max = value;
				maxItem = itemNo;
			}
			
		}
		
		// Print results
		System.out.printf("Max recommendations: %d for item %d\n"
				+ "Min recommendations: %d for item %d\n"
				+ "Average recommendations per item: %.2f\n"
				+ "Total items: %d\n"
				+ "Total recommendations: %d\n", min, minItem, max, maxItem, 
				(double)sum/reviewCountMap.size(), reviewCountMap.size(), sum);
		
	}


	private static void buildItemRecoMap(Map<Integer, Integer> reviewCountMap, String file) {

		try (BufferedReader reader = new BufferedReader(new FileReader(file))) {

			int lineCount = 0;
			
			while (reader.ready()) {

				if (lineCount++ % 1000000 == 0) {
					System.out.println(lineCount/1000000 + " million lines read.");
				}
				
				String line = reader.readLine();

				String[] lSplit = line.split("\\s+");

				insertMap(reviewCountMap, Integer.parseInt(lSplit[0]));		

			}

		} catch (IOException e) {
			e.printStackTrace();
		}

	}


	private static void printMapMin(Map<Integer, Integer> reviewCountMap, int min) {


		int count = 0;
		
		for (Integer itemNo : reviewCountMap.keySet()) {
			if (reviewCountMap.get(itemNo) < min) {
				count++;
				System.out.println("Item Number " + itemNo + " has less than minimum "
						+ "recommendations: " + reviewCountMap.get(itemNo));
			}
		}
		
		System.out.println(count + " items had less than " + min + " recommendations.");

	}


	private static void insertMap(Map<Integer, Integer> reviewCountMap,
			int item) {

		if (reviewCountMap.containsKey(item)) {
			reviewCountMap.put(item, reviewCountMap.get(item) + 1);
		} else {
			reviewCountMap.put(item, 1);
		}


	}

}
