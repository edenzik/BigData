import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

public class InputFilter {

	public static void main(String[] args) {

		inputToCSV("C:/Users/Michael/Desktop/all.txt", "allFilteredHelpful.csv", true);

	}

	private static void inputToCSV (String inPath, String outPath, boolean helpful) {

		try (BufferedReader reader = new BufferedReader(new FileReader(inPath));
				BufferedWriter writer = new BufferedWriter(new FileWriter(outPath))) {

			//Read lines 1,4,7 to CSV
			while (reader.ready()) {

				String product = reader.readLine();

				// Skip 2 lines
				skipLines(reader, 2);

				String user = reader.readLine();

				String helpfulness = null;

				if (!helpful) {
					// Skip 2 lines
					skipLines(reader, 2);
				} else {
					// Skip 1 line
					skipLines(reader, 1);
					helpfulness = reader.readLine();
				}

				String score = reader.readLine();

				// Skip remaining 4 lines
				skipLines(reader, 4);

				// Strip remaining label from lines
				product = product.substring(19, product.length());
				user = user.substring(15, user.length());
				score = score.substring(14, score.length());
				if (helpful) {
					helpfulness = helpfulness.substring(20, helpfulness.length());
				}

				// Write cleaned output
				if (!helpful) {
					writer.write(product + "," + user + "," + score + "\n");
				} else {
					writer.write(product + "," + user + "," + score + "," + helpfulness + "\n");
				}

			}





		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	private static void skipLines (BufferedReader reader, int lines) throws IOException {

		for (int i = 0; i < lines; i++) {
			if (reader.ready()) {
				reader.readLine();
			}
		}

	}

}
