package util;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class TitleProfessionParser {

	// Main method is only used for unit testing
	public static void main(String[] args) {
		try {
			BufferedReader br = new BufferedReader(new FileReader("profession_head.txt"));
			Map<String, Set<String>> map = buildTitleProfessionMap(br);
			System.out.println(map.keySet().iterator().next() + " " + map.get(map.keySet().iterator().next()));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static Map<String, Set<String>> buildTitleProfessionMap(BufferedReader br) throws IOException {
		Map<String, Set<String>> map = new HashMap<String, Set<String>>();

		String titleProfessionPair;
		while((titleProfessionPair = br.readLine()) != null){
			map.put(getTitle(titleProfessionPair), getProfessions(titleProfessionPair));
		}

		return map;
	}

	// Returns the title from a given title profession-list pair
	//
	// input: "[article title] : [list of comma-separated professions]"
	// output: "[article title]"
	public static String getTitle(String tpPair) {
		try {
			return AsciiToUnicode.parse(tpPair.split(":")[0]).trim();
		} catch(UnsupportedEncodingException e) {
			e.printStackTrace();
			return null;
		}
	}

	// Returns a list of professions from a given profession-list pair
	//
	// input: "[article title] : [list of comma-separated professions]"
	// output: List of professions
	public static Set<String> getProfessions(String tpPair) {
		String unparsedProfessions = tpPair.split(":")[1].trim();
		String[] professions = unparsedProfessions.split(",");
		return new HashSet<String>(Arrays.asList(professions));
	}
}
