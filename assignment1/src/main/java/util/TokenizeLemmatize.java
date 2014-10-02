package util;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;


/**
 * This class contains methods to parse, stem, and tokenize a given body of text.
 * To use this class, you may either pass the String text of the article as the sole
 * command line argument of the program, or you can call the public method
 * TokenizeLemmatize.parse(String article). For stemming, we used the Snowball Stemmer 
 * (http://snowball.tartarus.org/).
 * 
 * @author kahliloppenheimer
 * @since Oct 2, 2014
 */
public class TokenizeLemmatize {
	
	// set of all stop words as decided by STOP_WORD_FILE
	private static Set<String> stopWords = new HashSet<String>();
	// map of all lemma-frequency pairs
	private static Map<String, Integer> wordCount = new HashMap<String, Integer>();
	// Snowball Stemmer used to "lemmatize" each token
	private static final SnowballStemmer stemmer = new EnglishStemmer();
	
	/** Main class is used for unit testing only
	 * @param args may be used to pass the String text of an article
	 */
	public static Map<String, Integer> main(String[] args) {
		if(args.length == 1) {
			return parse(args[0]);
		}
		return null;
	}

	
	/** Parse plain text (with punctuation) input into key,value pairs of lemmas and frequencies
	 * 
	 * @param article String containing body text of article to be Tokenized and Lemmatized
	 * @return This returns a HashMap containing key,value pairs of lemmas and frequencies from the given text
	 */
	// Contains the logical outline of the parse job for this T.L. task
	public static Map<String, Integer> parse(String article) {

		// call this with actual title and article parameters
		stopWords = readStopWords(stopListText);

		// Make new wordCount for output
		wordCount = new HashMap<String, Integer>();

		// Drop case
		article = article.toLowerCase();

		// Clean text
		article = removeNonAlphabetic(article);

		// split on white space
		String[] whiteSpaceSeparated = splitOnWhiteSpace(article);
		
		// remove stopwords, stem remaining words using Snowball stemmer, and map results
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
	// output: a List of the stemmed words with any stop words filtered out
	//
	// Notably, this returns a List, but DOES modify the passed array to
	// be more space efficient
	private static String stemLocal(String token) {
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
		// Remove everything that's not a letter or whitespace
		s = s.replaceAll("[^a-z\\s]", " ");
		return s;
	}
	
	// input: File containing stop words, each on a new line
	// output: set of all those stop words
	private static Set<String> readStopWords(String[] list) {
		Set<String> stopWords = new HashSet<String>();
		
		for(String nextWord : list) {
			stopWords.add(nextWord);
		}

		return stopWords;
	}
	
	// input: list of all stemmed words in a given article
	// output: each word mapped to its number of appearances in that article
	private static void wordCount(String stem) {
		// makes sure that no nulls make it through
		if(stem == null) return;
		// weeds out the empty string as a stem
		if(stem.length() == 0) return;
		
		if(!wordCount.containsKey(stem)) {
			wordCount.put(stem, 1);
		} else {
			wordCount.put(stem, wordCount.get(stem) + 1);
		}

	}		//End of wordCount()
	
	static String[] stopListText = {"need", "thats", "using", "he'd", "said", "higher", "particular", "parts", "until", "becoming", 
			"over", "thanks", "began", "she", "thereby", "something", "right", "opened", "these", "asked", "else", 
			"once", "respectively", "number", "he", "theirs", "apart", "shows", "few", "further", "he's", "opening", 
			"herself", "downwards", "somebody", "each", "big", "wherever", "go", "t's", "she's", "before", "made", 
			"accordingly", "indicate", "parted", "namely", "needing", "interested", "six", "hereafter", "she'd", 
			"side", "could", "consider", "usually", "do", "tell", "interesting", "whither", "man", "member", 
			"look", "f", "thorough", "ex", "g", "d", "may", "e", "b", "noone", "c", "needs", "a", "n", "o", 
			"l", "m", "won", "j", "ones", "backing", "k", "h", "i", "yes", "w", "v", "eg", "u", "new", "t", 
			"s", "what", "r", "newer", "q", "p", "nothing", "having", "et", "z", "y", "yet", "x", "here's", 
			"thru", "anywhere", "least", "you'd", "took", "by", "long", "enough", "same", "has", "backs", "who", 
			"couldn", "would", "wanting", "facts", "any", "overall", "everybody", "had", "be", "think", "get", 
			"seeing", "likely", "far", "a's", "much", "and", "particularly", "co", "gotten", "near", "differently", 
			"i'd", "often", "better", "against", "containing", "doing", "areas", "seeming", "orders", "example", 
			"i'm", "make", "large", "thing", "room", "does", "shan", "saying", "ignored", "today", "tried", 
			"former", "through", "possible", "following", "area", "especially", "generally", "name", "showing", 
			"men", "edu", "tries", "members", "all", "sides", "keeps", "five", "obviously", "she'll", "at", 
			"as", "still", "neither", "hello", "therefore", "never", "great", "which", "see", "i'll", "am", 
			"anyone", "take", "an", "there", "off", "thoroughly", "why", "nobody", "they", "somehow", "no", 
			"you've", "nine", "otherwise", "ours", "ourselves", "anyways", "of", "help", "given", "asks", "among", 
			"youngest", "says", "only", "on", "anybody", "ok", "her", "everyone", "fully", "that's", "itself", 
			"oh", "thoughts", "maybe", "or", "done", "pointed", "regarding", "third", "sensible", "them", "then", 
			"will", "ought", "furthermore", "small", "novel", "upon", "different", "indeed", "getting", "thought", 
			"most", "thanx", "followed", "aside", "across", "clear", "looking", "thank", "normally", "furthers", 
			"unless", "where's", "rather", "me", "aren", "kept", "mr", "smallest", "beings", "don", "it's", 
			"my", "whereupon", "differ", "okay", "specified", "it'd", "per", "how's", "thinks", "nd", "sometime", 
			"pointing", "within", "thereupon", "furthered", "described", "truly", "follows", "you're", "cause", 
			"tends", "last", "second", "sometimes", "finds", "being", "newest", "contains", "since", "actually", 
			"him", "where", "every", "eight", "almost", "unto", "looks", "more", "his", "inc", "grouped", "we'd", 
			"when", "someone", "wonder", "value", "useful", "none", "certainly", "younger", "seriously", "everywhere", 
			"asking", "onto", "appropriate", "isn", "such", "c's", "hers", "liked", "whereafter", "here", 
			"presents", "whole", "this", "causes", "appreciate", "becomes", "goods", "way", "from", "hi", "believe", 
			"smaller", "while", "was", "ain", "allows", "able", "if", "corresponding", "ie", "seemed", "below", 
			"various", "wherein", "lest", "between", "less", "those", "is", "it", "besides", "ourselves", "gives", 
			"important", "your", "gets", "into", "problem", "howbeit", "in", "know", "two", "away", "felt", 
			"necessary", "things", "themselves", "lets", "also", "changes", "greater", "appear", "etc", "knew", 
			"they'll", "hopefully", "ours", "its", "yourselves", "turning", "showed", "exactly", "although", 
			"c'mon", "formerly", "interest", "greetings", "year", "it'll", "points", "entirely", "along", "place", 
			"secondly", "serious", "alone", "awfully", "turn", "going", "nowhere", "ends", "relatively", "how", 
			"under", "downed", "available", "became", "always", "indicated", "theres", "inward", "own", "specify", 
			"indicates", "try", "ways", "we", "reasonably", "face", "give", "specifying", "i've", "next", "states", 
			"use", "hardly", "vs", "consequently", "mrs", "when's", "numbers", "older", "worked", "whenever", 
			"best", "mostly", "definitely", "unfortunately", "whatever", "we'll", "later", "back", "come", "us", 
			"seen", "young", "un", "cannot", "seem", "works", "up", "downing", "gave", "either", "fact", "presenting", 
			"seconds", "insofar", "sorry", "doesn", "they'd", "down", "part", "happens", "keep", "to", "faces", 
			"com", "both", "inner", "uucp", "become", "you'll", "good", "ended", "somewhere", "must", "parting", 
			"th", "didn", "after", "nevertheless", "whereby", "who's", "considering", "sees", "ordering", 
			"taken", "welcome", "presented", "what's", "however", "so", "whose", "behind", "gone", "places", 
			"willing", "that", "whereas", "associated", "than", "several", "thence", "unlikely", "whom", "case", 
			"ltd", "got", "oldest", "early", "hereby", "sub", "can", "about", "well", "re", "sup", "longest", 
			"rd", "above", "que", "qv", "four", "placed", "too", "yours", "furthering", "thus", "moreover", 
			"provides", "you", "soon", "needed", "general", "immediate", "anything", "seven", "ordered", "whoever", 
			"high", "certain", "latest", "somewhat", "our", "brief", "out", "very", "forth", "via", "hereupon", 
			"for", "everything", "towards", "zero", "whether", "beyond", "elsewhere", "went", "course", "open", 
			"whence", "are", "grouping", "can", "shouldn", "yourself", "working", "groups", "rooms", "therein", 
			"thereafter", "plus", "problems", "others", "we're", "mainly", "viz", "again", "did", "wasn", 
			"like", "without", "non", "shall", "not", "many", "present", "he'll", "nor", "haven't", "anyhow", 
			"now", "cant", "backed", "say", "myself", "saw", "years", "ask", "some", "why's", "outside", "might", 
			"put", "self", "trying", "wanted", "kind", "according", "they've", "seems", "twice", "latter", "presumably", 
			"probably", "inasmuch", "end", "want", "regardless", "just", "hence", "fifth", "cases", "let", "evenly", 
			"already", "should", "wouldn", "point", "really", "beforehand", "mustn", "clearly", "despite", 
			"hither", "old", "but", "afterwards", "meanwhile", "herein", "wish", "hadn", "amongst", "little", 
			"show", "used", "been", "though", "together", "hasn", "anyway", "sent", "were", "turned", "please", 
			"toward", "puts", "there's", "three", "longer", "concerning", "sure", "work", "throughout", "except", 
			"goes", "regards", "we've", "comes", "himself", "wants", "knows", "contain", "latterly", "even", 
			"known", "perhaps", "ever", "wells", "other", "allow", "interests", "have", "highest", "one", "state", 
			"selves", "currently", "turns", "merely", "let's", "because", "another", "order", "full", "during", 
			"mean", "lately", "making", "they're", "find", "weren", "with", "greatest", "nearly", "opens", 
			"came", "the", "ending", "around", "beside", "quite", "largely", "instead", "downs", "uses", "group", 
			"their", "first"};
	
}	//End of Class
