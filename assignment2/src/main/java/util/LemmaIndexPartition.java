//package util;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.lang.Math;


/**
* Reads in a lemma frequency pair files (Philipp Moog	<german,2>,<die,1>,<philipp,1>)
* Reads in people occuptation file (Paul Francis Anderson : catholic bishop)
* Subtracts from lemma frequency pair file all those which are not in people occuptation file.
* Accepts 2 or more arguments: 
* java LemmaIndexPartition lemmaFreqPair_file.txt personProfession_file.txt [frequency distribution (ex. 50)] [output_file1.txt] [output_file2.txt]
* java LemmaIndexPartition ../take_home/lemma_index_aggregate ../conversion/profession_train_converted_to_utf_8.txt 50 file1.txt file2.txt
* 
* @author edenzik
* @since Oct 13, 2014
*/
public class LemmaIndexPartition {
	
	/** Main class is used for unit testing only
	 * @param args may be used to pass an ascii character
	 * @return unicode character
	 */
	public static void main(String[] args) throws UnsupportedEncodingException, IOException{
		BufferedReader personProfession = null;
		BufferedReader lemmaFreq = null;
		ArrayList<PrintWriter> writers = null;
		if (args.length >= 2) {
			lemmaFreq = new BufferedReader(new FileReader(args[0]));
			personProfession = new BufferedReader(new FileReader(args[1]));
			
			writers = new ArrayList<PrintWriter>();
			int dist = 100;
			
			if (args.length > 2){
				dist = Integer.parseInt(args[2]);
				for (int arg = 3; arg<args.length; arg++){
					writers.add(new PrintWriter(args[arg], "UTF-8"));
				}
			} else {
				writers.add(new PrintWriter(System.out));
			}
		
			HashSet<String> desiredPeople = parsePersonProfession(personProfession);
			parseLemmaPersonFreq(lemmaFreq, desiredPeople, writers, dist);
			
		} else System.out.println("Please specify the location of the inverted index and the profession file.");	
		personProfession.close();
		lemmaFreq.close();
		for (PrintWriter f : writers){
			f.close();
		}
		
	}
	
	public static String toLemmaFreqPairs(ArrayList<SimpleEntry<String,Integer>> personFreqList){
		StringBuilder sb = new StringBuilder();
		for (SimpleEntry<String,Integer> entry : personFreqList){
			sb.append("<" + entry.getKey() + "," + entry.getValue() + ">");
		}
		return sb.toString().replace("><",">,<");
	}
	
	public static String toLemmaFreq(SimpleEntry<String,ArrayList<SimpleEntry<String,Integer>>> lemmaPersonFreq){
		String output = lemmaPersonFreq.getKey() + "\t" + toLemmaFreqPairs(lemmaPersonFreq.getValue());
		return output;
	}

	public static void parseLemmaPersonFreq(BufferedReader input, HashSet<String> desiredPeople, ArrayList<PrintWriter> writers, int dist) throws IOException{
		//Map<String,ArrayList<SimpleEntry<String,Integer>>> lemmaPeople = new HashMap<String,ArrayList<SimpleEntry<String,Integer>>>();
		String line = "";
		while ((line = input.readLine()) != null) {
			SimpleEntry<String,ArrayList<SimpleEntry<String,Integer>>> entry = parseLemmaPersonFreqLine(line);
			
			if (personExists(entry.getKey(), desiredPeople)) randomWrite(toLemmaFreq(entry), writers, dist);
		}
	}
	
	public static void randomWrite(String line, ArrayList<PrintWriter> writers, int dist){
		Random generator = new Random(); 
		int random = generator.nextInt(100);
		random = random + dist;
		random = random/100;
		writers.get(random).println(line);
	}

	public static SimpleEntry<String,ArrayList<SimpleEntry<String,Integer>>> parseLemmaPersonFreqLine(String line){
		String[] lemmaValues = line.split("\t");
		ArrayList<SimpleEntry<String,Integer>> peopleFreq = new ArrayList<SimpleEntry<String,Integer>>();
		Pattern p = Pattern.compile("<([\\w+\\s]+,\\d+)>");
		if (lemmaValues.length>=2){
			Matcher m = p.matcher(lemmaValues[1]);
			while (m.find()){
				String[] nameFreqPair = m.group(1).split(",");
				SimpleEntry<String,Integer> nameFreqPairEntry = new SimpleEntry<String,Integer>(nameFreqPair[0],Integer.parseInt(nameFreqPair[1]));
				peopleFreq.add(nameFreqPairEntry);
			}
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

	public static SimpleEntry<String,ArrayList<SimpleEntry<String,Integer>>> eliminatePersonsEntry(SimpleEntry<String,ArrayList<SimpleEntry<String,Integer>>> lemmaEntry, HashSet<String> desiredPeople){
		return new SimpleEntry<String,ArrayList<SimpleEntry<String,Integer>>>(lemmaEntry.getKey(), eliminatePersonsList(lemmaEntry.getValue(),desiredPeople));
	}

	public static ArrayList<SimpleEntry<String,Integer>> eliminatePersonsList(ArrayList<SimpleEntry<String,Integer>> peopleList, HashSet<String> desiredPeople){
		ArrayList<SimpleEntry<String,Integer>> revisedList = new ArrayList<SimpleEntry<String,Integer>>();
		for (SimpleEntry<String,Integer> person : peopleList){
			if (personExists(person.getKey(),desiredPeople)) {
				revisedList.add(person);
			}
		}
		return revisedList;
	}

	public static boolean personExists(String person, HashSet<String> desiredPeople){
		return desiredPeople.contains(person);
	}


	

	
}