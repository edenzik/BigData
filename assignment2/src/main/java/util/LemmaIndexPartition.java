package util;

import java.net.URLDecoder;
import java.io.UnsupportedEncodingException;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;
import java.io.FileReader;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.util.HashMap;
import java.util.Map;
import java.util.HashSet;

/**
 * This class converts character in ASCII to one in unicode.
 * Example: Vladim%C3%ADr Leitner -> Vladimdr Leitner (with a little dash over the A)
 * @author edenzik
 * @since Oct 9, 2014
 */
public class LemmaIndexPartition {
	
	/** Main class is used for unit testing only
	 * @param args may be used to pass an ascii character
	 * @return unicode character
	 */
	public static void main(String[] args) throws UnsupportedEncodingException, IOException{
		if (args.length == 2) {
			BufferedReader input = new BufferedReader(new FileReader(args[0]));
			String s;
		} else System.out.println("Please specify the location of the inverted index and the profession file.");

	}

	public static Map<String,ArrayList<SimpleEntry<String,Integer>>> parseLemmaPersonFreq(BufferedReader input, HashSet desiredPeople) throws IOException{
		Map<String,ArrayList<SimpleEntry<String,Integer>>> lemmaPeople = new HashMap<String,ArrayList<SimpleEntry<String,Integer>>>();
		String line = "";
		while ((line = input.readLine()) != null) {
			SimpleEntry<String,ArrayList<SimpleEntry<String,Integer>>> entry = eliminatePersonsEntry(parseLemmaPersonFreqLine(line), desiredPeople);
			lemmaPeople.put(entry.getKey(), entry.getValue());
		}
		return lemmaPeople;
	}

	public static SimpleEntry<String,ArrayList<SimpleEntry<String,Integer>>> parseLemmaPersonFreqLine(String line){
		String[] lemmaValues = line.split("\t");
		ArrayList<SimpleEntry<String,Integer>> peopleFreq = new ArrayList<SimpleEntry<String,Integer>>();
		Pattern p = Pattern.compile("<([\\w+\\s]+,\\d+)>");
		Matcher m = p.matcher(lemmaValues[1]);
		while (m.find()){
			String[] nameFreqPair = m.group(1).split(",");
			SimpleEntry<String,Integer> nameFreqPairEntry = new SimpleEntry<String,Integer>(nameFreqPair[0],Integer.parseInt(nameFreqPair[1]));
			peopleFreq.add(nameFreqPairEntry);
		}
		SimpleEntry<String,ArrayList<SimpleEntry<String,Integer>>> entry = new SimpleEntry<String,ArrayList<SimpleEntry<String,Integer>>>(lemmaValues[0],peopleFreq);
		return entry;
	}


	public static HashSet<String> parsePersonProfession(BufferedReader input) throws IOException{
		HashSet<String> desiredPeople = new HashSet<String>();
		String line = "";
		while ((line = input.readLine()) != null) {
			desiredPeople.add(parsePersonProfessionLine(line));
		}
		return desiredPeople;
	}

	public static String parsePersonProfessionLine(String line){
		return line.split(":")[0].trim();
	}

	public static SimpleEntry<String,ArrayList<SimpleEntry<String,Integer>>> eliminatePersonsEntry(SimpleEntry<String,ArrayList<SimpleEntry<String,Integer>>> lemmaEntry, HashSet desiredPeople){
		eliminatePersonsList(lemmaEntry.getValue(),desiredPeople);
		return lemmaEntry;
	}

	public static void eliminatePersonsList(ArrayList<SimpleEntry<String,Integer>> peopleList, HashSet desiredPeople){
		for (SimpleEntry<String,Integer> person : peopleList){
			if (!personExists(person.getKey(),desiredPeople)) {
				peopleList.remove(person);
			}
		}
	}

	public static boolean personExists(String person, HashSet desiredPeople){
		return desiredPeople.contains(person);
	}


	

	
}