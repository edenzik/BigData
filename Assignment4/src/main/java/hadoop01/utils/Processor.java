package hadoop01.utils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.AbstractMap.SimpleEntry;
import java.util.Comparator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;



/**
 * @author edenzik
 * 
 * Reads in this type of data:
 * 2440602	2440899	1.0
2440605	2440704	1.0
2440622	2440774	1.0
2440640	2440766	1.0
2440659	2440937	1.0
2440732	2440829	1.0
2440732	2440899	1.0
2440829	2440899	1.0
2440913	2440988	0.5
2440924	2440972	1.0
 *
 *Puts it into a hashmap, if a mapping from element1-element2 is not bidirectional, 
 *makes it so.
 */
public class Processor {

	public static void main(String[] args) throws IOException, InterruptedException {
		reader(args[0]);
	}
	
	static void reader(String file) throws IOException, InterruptedException{
		final BufferedReader br = new BufferedReader(new FileReader(file));
		final BufferedWriter bw = new BufferedWriter(new FileWriter(file+"_output.txt"));
		String l;
		final Map<Number, Set<SimpleEntry<Number, Number>>> allElements = new ConcurrentHashMap<Number, Set<SimpleEntry<Number, Number>>>();
		int counter = 0;
		final compareElement compare = new compareElement();
		while ((l = br.readLine())!=null){
			counter++;
			Number[] currentItem = parseLine(l);
			final Number elementKey = currentItem[0];
			final Number elementValue = currentItem[1];
			final Number elementStrength = currentItem[2];
			
			
						if (allElements.containsKey(elementKey)){
							//System.out.println("SHF");
							Set<SimpleEntry<Number,Number>> list = allElements.get(elementKey);
							list.add(new SimpleEntry<Number,Number>(elementValue, elementStrength));
						} else {
							//System.out.println("MOOSOMETIME");
							Set<SimpleEntry<Number,Number>> list = new TreeSet<SimpleEntry<Number,Number>>(compare);
							list.add(new SimpleEntry<Number,Number>(elementValue, elementStrength));
							allElements.put(elementKey, list);
						}
						//System.out.println(elementValue);
					   if (allElements.containsKey(elementValue)){
						   Set<SimpleEntry<Number,Number>> list = allElements.get(elementValue);
						   list.add(new SimpleEntry<Number,Number>(elementKey, elementStrength));
					   } else {
						   Set<SimpleEntry<Number,Number>> list = new TreeSet<SimpleEntry<Number,Number>>(compare);
							list.add(new SimpleEntry<Number,Number>(elementKey, elementStrength));
							allElements.put(elementValue, list);
					   }
			
			if (counter%100000 ==0) System.out.println((counter*100)/13798685 + " Total Lines" + " SIZE: " + allElements.size());
		}

		
		 for (Entry<Number, Set<SimpleEntry<Number, Number>>> e : allElements.entrySet()){
		   for (SimpleEntry<Number, Number> s : e.getValue()){
			   try {
				   bw.write(e.getKey().toString() + "\t" + s.getKey().toString() + "\t" + s.getValue().toString() + "\n");
			   } catch (IOException e1) {
				   // TODO Auto-generated catch block
				   e1.printStackTrace();
			   }
		   }
		 }
	 //  }
	   //writtenToDisk.add((Integer) elementKey);
//   }
			br.close();
			bw.close();

	}
	
	static Number[] parseLine(String line){
		String[] items = line.split("\t");
		return new Number[]{new Integer(items[0].trim()), new Integer(items[1].trim()), new Double(items[2].trim())};
	}
	
	static class compareElement implements Comparator<SimpleEntry<Number,Number>>{
		public int compare(SimpleEntry<Number, Number> o1,
				SimpleEntry<Number, Number> o2) {
			return o1.getKey().intValue() - o2.getKey().intValue();
		}
		
	}
	
}

