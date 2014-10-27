package hadoop01.utils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashSet;

public class StopFilter {

	public static void main(String[] args) {

		HashSet<String> stopSet = new HashSet<String>(100000);

		try {
			BufferedReader stopReader = new BufferedReader(new FileReader(args[0]));


			while (stopReader.ready()) {

				String next = stopReader.readLine();
				stopSet.add(next);	

			}
			stopReader.close();

			BufferedReader inputReader = new BufferedReader(new FileReader(args[1]));
			FileWriter writer = new FileWriter(args[2]);

			while (inputReader.ready()) {

				String nextLine = inputReader.readLine().toLowerCase();
				nextLine = nextLine.replaceAll("[^a-z ]", " ");
				for (String word : nextLine.split(" ")) {
					if (!word.equals(" ") && !word.equals("")) {
						writer.write(word + " ");
					}

				}

				writer.write("\n");
			}

		} catch (IOException e) {
			e.printStackTrace();
		}


	}

}
