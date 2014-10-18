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
		Map<String, Map<String, Double>> professionLemmaMap = buildMap(br);
		
	}
	
	public static Map<String, Map<String, Double>> buildMap(BufferedReader br) throws IOException {
		Map<String, Map<String, Double>> professionLemmaMap = new HashMap<>();
		String nextLine = null;
		while((nextLine = br.readLine()) != null) {
			String profession = nextLine.split("\t")[0].trim();
			String lemmaFreqText = nextLine.split("\t")[1].trim();
			Map<String, Double> lemmaFreq = parseLine(lemmaFreqText);
			professionLemmaMap.put(profession, lemmaFreq);
		}
		return professionLemmaMap;
	}
	
	public static Map<String, Double> parseLine(String lemmaFreqText) {
		lemmaFreqText = lemmaFreqText.trim();
		Map<String, Double> lemmaFreq = new HashMap<>();
		Scanner sc = new Scanner(lemmaFreqText);
		
		return lemmaFreq;
	}

}
