import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;


/**
 * @author kahliloppenheimer
 *
 */
public class TokenizeLemmatize {
	
	//file of stop words
	private static final String STOP_WORD_FILE="stoplists/aggregate.txt";
	
	// set of all stop words as decided by STOP_WORD_FILE
	private static Set<String> stopWords = new HashSet<String>();
	// map of all lemma-frequency pairs
	private static Map<String, Integer> wordCount = new HashMap<String, Integer>();
	
	
	/** Main class is used for unit testing only
	 * @param args Parameter is not used
	 */
	public static void main(String[] args) {

		// place holder for real article text
		final String TEST_ARTICLE="Well, this is an      article \t\tabout\r\r foo and bar. \nBut what we're all really " +
				"wondering is why foo? \nAnd why bar? Why not just continue on like jolly old souls, Mr.Foo and Mrs.Bar?";

		parse(TEST_ARTICLE);

		//This block for unit testing:
//		Iterator<Entry<String, Integer>> output =  wordCount.entrySet().iterator();
//		while (output.hasNext()) {
//			Entry<String, Integer> e = output.next();
//			System.out.println(e.getKey() + " " + e.getValue());
//		}
		
	}

	
	/** Parse plain text (with punctuation) input into key,value pairs of lemmas and frequencies
	 * 
	 * @param article String containing body text of article to be Tokenized and Lemmatized
	 * @return This returns a HashMap containing key,value pairs of lemmas and frequencies from the given text
	 */
	// Contains the logical outline of the parse job for this T.L. task
	public static Map<String, Integer> parse(String article) {

		// call this with actual title and article parameters
		stopWords = readStopWords(new File(STOP_WORD_FILE));

		//Make new wordCount for output
		wordCount = new HashMap<String, Integer>();

		//Drop case and clean text
		article = removeNonAlphabetic(article);

		// split on white space
		String[] whiteSpaceSeparated = splitOnWhiteSpace(article);
		
		// stem words using Snowball stemmer and map results
		for (String token : whiteSpaceSeparated) {
			String lemma = stemLocal(token);
			if (lemma != null) {
				//Add this stem to the map
				wordCount(lemma);
			}
				
		}
		
		return wordCount;
		
	}
	
	// input: one string containing an entire article
	// output: that article split into "tokens" solely based on whitespace
	private static String[] splitOnWhiteSpace(String article) {
		return article.split("\\s+");
	}
	
	// input: an array of non-whitespace Strings
	// output: a List of the stemmed words
	//
	// Notably, this returns a List, but DOES modify the array to
	// be more memory efficient
	private static String stemLocal(String token) {
		SnowballStemmer stemmer = new EnglishStemmer();
					
		// Filters out any "stop words"
		if(!stopWords.contains(token)) {
			stemmer.setCurrent(token);
			stemmer.stem();
			return stemmer.getCurrent();
		} else {
			return null;
		}

	}
	
	// input: a string representing a single token (i.e. no white space)
	// output: a string with all non-alphabetic characters removed
	private static String removeNonAlphabetic(String s) {
		//Drop case
		s = s.toLowerCase();
		
		//Smart remove punctuation, keeping *'t, removing *'s including s
		s = s.replaceAll("'s", "");
		s = s.replaceAll("[^a-z \"'t\"]", " ");
		
		return s;
	}
	
	// input: File containing stop words, each on a new line
	// output: set of all those stop words
	private static Set<String> readStopWords(File f) {
		Set<String> stopWords = new HashSet<String>();
		try (BufferedReader br = new BufferedReader(new FileReader(f))) {
			String nextWord = null;
			while((nextWord = br.readLine()) != null) {
				stopWords.add(nextWord);
			}
		} catch(IOException e) {
			e.printStackTrace();
		}
		return stopWords;
	}
	
	// input: list of all stemmed words in a given article
	// output: each word mapped to its number of appearances in that article
	private static void wordCount(String stem) {
		if(!wordCount.containsKey(stem)) {
			wordCount.put(stem, 1);
		} else {
			wordCount.put(stem, wordCount.get(stem) + 1);
		}

	}		//End of wordCount()
	
	
}	//End of Class
