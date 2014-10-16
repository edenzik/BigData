package util;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.HashMap;
import java.util.Arrays;

/**
 * Reads in a name profession pair:
 *
 * James Smith (sports media figure) : television personality
 * Jackson Allen : rules footballer
 * Teamwork Motion Pictures : film actor, actor
 * Shyla Stylez : pornographic film actor, film actor, actor
 * Gabriel Elorde : boxer
 * Manoj David : cricketer
 * Eugenio Fernando Bila : footballer
 * Roger Blonder : animator
 * David Brown (scientist) : biologist, geneticist
 * And another name profession pair from a different file:
 * Accepts 2 or more arguments:
 * java ProfessionMatchEvaluator personProfessionControl.txt personProfessionTest.txt outputResult.txt
 * Example from the evaluate folder:
 * java ProfessionMatchEvaluator run006_agg.txt ../conversion/profession_train_converted_to_utf_8.txt compare_1.txt
 *
 * @author edenzik
 * @since Oct 13, 2014
 */
public class ProfessionMatchEvaluator {

	/** Main class is used for unit testing only
	 * @param args may be used to pass an ascii character
	 * @return unicode character
	 */
	public static void main(String[] args) throws UnsupportedEncodingException, IOException{
		BufferedReader personProfessionControl = null;
		BufferedReader personProfessionTest = null;
		PrintWriter writer = null;
		if (args.length >= 3) {
			personProfessionControl = new BufferedReader(new FileReader(args[0]));
			personProfessionTest = new BufferedReader(new FileReader(args[1]));
			writer = new PrintWriter(args[2], "UTF-8");

			HashMap<String, HashSet<String>> controlPeople = parsePersonProfession(personProfessionControl);
			HashMap<String, HashSet<String>> testPeople = parsePersonProfession(personProfessionTest);

			comparePersonProfessions(testPeople, controlPeople, writer);

			personProfessionControl.close();
			personProfessionTest.close();
			writer.close();

		} else System.out.println("Please specify the location of the inverted index and the profession file.");
		
	}

	public static void comparePersonProfessions(HashMap<String, HashSet<String>> first, HashMap<String, HashSet<String>> second, PrintWriter writer){
		int[] result;
		int hits = 0;
		int correctGuess = 0;
		int totalProf = 0;
		int total = 0;
		for (Map.Entry<String,HashSet<String>> person : first.entrySet()){
			String personName = person.getKey();
			if (second.containsKey(person.getKey())) {
				result = comparePersonProfessions(person.getValue(),second.get(person.getKey()));
				if (result[0]>0) hits++;
				correctGuess+=result[0];
				totalProf+=result[1];
				writer.println(personName + " : " + Arrays.toString(result));
				total++;
			} else {
				System.out.println(person.getKey() + " does not exist in control file");
			}
		}

		for (int i=0; i<10; i++) writer.print("=");
		writer.println("\nTA MEASURE");
		writer.println("HITS:\t" + hits);
		writer.println("RATIO:\t" + (double) hits/ (double) total);
		for (int i=0; i<10; i++) writer.print("-");
		writer.println("\nCORRECT GUESS");
		writer.println("CORRECT GUESSES:\t" + correctGuess);
		writer.println("TOTAL PROFESSIONS:\t" + totalProf);
		writer.println("RATIO:\t" + (double) correctGuess/ (double) totalProf);
	}

	public static int[] comparePersonProfessions(HashSet<String> first, HashSet<String> second){
		int match = 0;
		int total = first.size();
		for (String profession : first){
			if (second.contains(profession)) match++;
		}
		int[] result = {match, total};
		return result;
	}

	public static HashMap<String, HashSet<String>> parsePersonProfession(BufferedReader input) throws IOException{
		HashMap<String, HashSet<String>> peopleProfession = new HashMap<String, HashSet<String>>();
		String line = "";
		while ((line = input.readLine()) != null) {
			line = line.replace("\t"," : ");
			peopleProfession.put(parsePersonLine(line), parseProfessionLine(line));
		}
		return peopleProfession;
	}

	public static String parsePersonLine(String line){
		return line.split(":")[0].trim();
	}

	public static HashSet<String> parseProfessionLine(String line){
		HashSet<String> professions = new HashSet<String>();
		for (String profession : line.split(":")[1].trim().split(",")){
			professions.add(profession);
		}
		return professions;
	}





}

