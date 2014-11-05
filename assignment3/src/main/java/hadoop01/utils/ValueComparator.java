package hadoop01.utils;

import java.util.Comparator;
import java.util.Map.Entry;

public class ValueComparator implements Comparator<Entry<Integer, Double>> {

	public ValueComparator() {

	}

	public int compare(Entry<Integer, Double> e1, Entry<Integer, Double> e2) {
		if (e1.getValue() < e2.getValue()) {
			return 1;
		} else if (e1.getValue() == e2.getValue()) {
			return 0;
		} else {
			return -1;
		}

	}
}