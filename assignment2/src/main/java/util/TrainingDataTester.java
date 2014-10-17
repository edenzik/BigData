package util;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

public class TrainingDataTester {

	private static final String TRAINING_DATA_PATH="training_data";

	public static void main(String[] args) throws IOException {
		BufferedReader br = new BufferedReader(new FileReader(TRAINING_DATA_PATH));
		String nextLine = null;
		Map<String, String> professionMap = new HashMap<String, String>();
		List<String> professions = new ArrayList<String>();
		while((nextLine = br.readLine()) != null) {
			Scanner sc = new Scanner(nextLine);
			sc.useDelimiter("\t");
			String profession = sc.next();
			if(professionMap.get(profession) == null) {
				professionMap.put(profession, profession.trim());
			} else {
				throw new Error("Same profession show up twice! " + profession);
			}
			professions.add(profession.trim());
			sc.close();
		}
		System.out.println(professions);
		br.close();
		if(professionMap.size() != professions.size()) {
			throw new Error();
		}
	}

}
