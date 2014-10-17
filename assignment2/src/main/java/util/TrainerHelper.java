package util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import util.StringIntegerList.StringInteger;

public class TrainerHelper {
	
	// Main method is used for unit testing only
	public static void main(String[] args) {
		Map<String, Double> temp = new HashMap<String, Double>();
		temp.put("cat", 3.0);
		temp.put("dog", 5.0);
		temp.put("foo", 2.0);
		//System.out.println(getFrequencySum(temp));
		assert getFrequencySum(temp) == 10.0 : "denominator is not correct";
		
		StringInteger s1 = new StringInteger("cat", 3);
		StringInteger s2 = new StringInteger("dog", 5);
		StringInteger s3 = new StringInteger("foo", 1);
		StringInteger s4 = new StringInteger("foo", 1);
		
		List<StringInteger> list1 = new ArrayList<StringInteger>();
		List<StringInteger> list2 = new ArrayList<StringInteger>();
		list1.add(s1);
		list1.add(s2);
		list1.add(s4);
		list2.add(s3);
		StringIntegerList sil1 = new StringIntegerList(list1);
		StringIntegerList sil2 = new StringIntegerList(list2);
		List<StringIntegerList> lsil= new ArrayList<StringIntegerList>();
		lsil.add(sil1);
		lsil.add(sil2);
		//System.out.println(getAggregateMap(lsil));
		//System.out.println(getFrequencySum(getAggregateMap(lsil)));
		assert getFrequencySum(getAggregateMap(lsil)) == 10.0 : "aggregate map is not correct";
		
	}

	// Takes a Collection of StringIntegerLists and aggregates into one map
	// so that each lemma has one entry associated with the sum of each of its
	// frequencies with each different article of the same profession
	public static Map<String, Double> getAggregateMap(
			Iterable<StringIntegerList> profAndFreqs) {
		Map<String, Double> map = new HashMap<String, Double>();
		for(StringIntegerList l: profAndFreqs) {
			List<StringInteger> list = l.getIndices();
			for(StringInteger i: list) {
				if(map.get(i.getString()) == null) {
					map.put(i.getString(), (double) i.getValue());
				} else {
					map.put(i.getString(), map.get(i.getString()) + i.getValue());
				}
			}
		}
		return map;
	}
	
	// Iterates through the given lemma frequency map and sums up the frequencies
	// of all of the lemmas
	public static double getFrequencySum(Map<String, Double> lemmaFreqMap) {
		double denominator = 0.0;
		Set<String> keys = lemmaFreqMap.keySet();
		for(String s: keys) {
			denominator += lemmaFreqMap.get(s);
		}
		return denominator;
	}
	
}
