import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 
 */

/**
 * @author kahliloppenheimer
 *
 */
public class Tradoop {
	
	// place holder for actual file of stop words
	private static final String STOP_WORD_FILE="src/stopWordTest.txt";
	// place holder for real article title
	private static final String TEST_TITLE="foo";
	// place holder for real article text
	private static final String TEST_ARTICLE="Well, this is an article about foo and bar. But what we're all really wondering is why foo? And why bar? Why not just continue on like jolly old souls, Mr.Foo and Mrs.Bar?";
	// set of all stop words as decided by STOP_WORD_FILE
	private static Set<String> stopWords = new HashSet<String>();
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// call this with actual title and article parameters
		stopWords = readStopWords(new File(STOP_WORD_FILE));
		traMap(TEST_TITLE, TEST_ARTICLE);
	}
	
	// Contains the logical outline of the Tramap job for this Tradoop task
	public static void traMap(String title, String article) {
		// split on white space
		String[] whiteSpaceSeparated = splitOnWhiteSpace(article);
		// stem words using Snowball stemmer
		List<String> stemmed = stem(whiteSpaceSeparated);
		// map each word to its # of occurrences
		Map<String, Integer> wordCount = wordCount(stemmed);
		
		// maps the article title to the word count map
		Map<String, Map<String, Integer>> finalMap = new HashMap<String, Map<String, Integer>>();
		finalMap.put(title, wordCount);
		
		System.out.println(finalMap);
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
	private static List<String> stem(String[] whiteSpaceSeparated) {
		SnowballStemmer stemmer = new EnglishStemmer();
		List<String> stemmed = new ArrayList<String>();
		for(int i = 0; i < whiteSpaceSeparated.length; ++i) {
			String s = whiteSpaceSeparated[i].toLowerCase();
			whiteSpaceSeparated[i] = null;
			s = removeNonAlphabetic(s);
			// Filters out any "stop words"
			if(!stopWords.contains(s)) {
				stemmer.setCurrent(s);
				stemmer.stem();
				stemmed.add(stemmer.getCurrent());
			}
		}
		return stemmed;
	}
	
	// input: a string representing a single token (i.e. no white space)
	// output: a string with all non-alphabetic characters removed
	private static String removeNonAlphabetic(String s) {
		StringBuilder sb = new StringBuilder();
		for(int i = 0; i < s.length(); ++i) {
			char c = s.charAt(i);
			if(Character.isAlphabetic(c)) {
				sb.append(c);
			}
		}
		return sb.toString();
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
	private static Map<String, Integer> wordCount(List<String> stemmed) {
		Map<String, Integer> wordCount = new HashMap<String, Integer>();
		for(String s: stemmed) {
			if(!wordCount.containsKey(stemmed)) {
				wordCount.put(s, 1);
			} else {
				wordCount.put(s, wordCount.get(s) + 1);
			}
		}
		return wordCount;
	}
}
