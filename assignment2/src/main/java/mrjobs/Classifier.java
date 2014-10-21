package mrjobs;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import util.Profession;
import util.StringIntegerList;
import util.StringIntegerList.StringInteger;


/**
 * This class is used to classify articles passed in based on training previously done
 */
public class Classifier {

	private static String DEFAULT_TRAINING_PATH = "hdfs://deerstalker.cs.brandeis.edu:54645/user/hadoop01/output/old_training/part-r-00000";
	private static int OUTPUT_PROFESSION_NUMBER = 3;
	private static final double PENALTY = -35.0;

	/**
	 * Classifier uses training data to classify people
	 * input: Lemma Index of format <Title, <Lemma, Freq> > from assignment 1
	 * output: Profession Index of format <Title, <Profession 1, Profession 2, Profession 3> >
	 * Note: Output will be in Text, Text format.
	 * Note: Optional third argument is training data. If left blank, internal field will be used.
	 * 
	 * @param args args[0] is input directory, args[1] is output directory, (optional) args[2] is training data
	 * @throws Exception for any exception
	 */
	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "classify");
		job.setJarByClass(Classifier.class);
		job.setMapperClass(ClassifyMapper.class);
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		//Allows passing training data reference from command line
		if(args.length > 2){

			String pathString = "hdfs://deerstalker.cs.brandeis.edu:54645/user/hadoop01/" + args[2] + "/part-r-00000";
			job.addCacheFile(new Path(pathString).toUri());

			//If no command line reference specified, use standard one
		} else {

			job.addCacheFile(new Path(DEFAULT_TRAINING_PATH).toUri());

		}

		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}	//End of main()

	/**
	 * Mapper class performs all classifying actions and outputs to reducer only for consolidating output to one file
	 * @author Michael Partridge
	 */
	public static class ClassifyMapper extends Mapper<Text, Text, Text, Text> {

		//This map will contain all training data for classification
		//Setup method populates this map and map method uses it
		private HashMap<String, Map<String, Double>> fullProfessionMap;

		@Override
		protected void setup(Mapper<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {

			super.setup(context); 

			//Builds a map of people->profession
			URI[] files = Job.getInstance(context.getConfiguration()).getCacheFiles();
			FileSystem fs = FileSystem.get(context.getConfiguration());

			BufferedReader reader = new BufferedReader( new InputStreamReader( fs.open(new Path(files[0])) ) );

			//BufferedReader reader = new BufferedReader( new FileReader(data_path));
			fullProfessionMap = buildJobMapWithoutRFS(reader);

			reader.close();

		}

		/**
		 * map method uses the profession data map created in setup to classify each person (key)
		 */
		@Override
		public void map(Text title, Text listText, Context context)
				throws IOException, InterruptedException {

			//Build a list of lemmaa-frequency pairs for classifying
			StringIntegerList lemmaFreq = new StringIntegerList();
			lemmaFreq.readFromString(listText.toString());
			List<StringInteger> lemmaList = lemmaFreq.getIndices();

			//These hold the top (so far) 3 professions and probabilities for this person for final outputting
			String[] topNames = new String[OUTPUT_PROFESSION_NUMBER];
			double[] topProbabilities = new double[OUTPUT_PROFESSION_NUMBER];

			//Set topProbabilities to negative infinity
			//This way any new value compared will be greater
			for (int i = 0; i < topProbabilities.length; i++) {
				topProbabilities[i] = Double.NEGATIVE_INFINITY;
			}

			//Loop through each possible profession, and calculate the probability for this person
			for (Profession profession : Profession.values()) {

				//Set initial probability (log value) to 0 and get cloned map of this data
				double totalP = 0;
				Map<String,Double> trainingMap = fullProfessionMap.get(profession.getName());

				//Did we have data for this profession in training data?
				if (trainingMap != null) {

					//For each lemma in this list, add the probability 
					for (StringInteger strInt : lemmaList) {

						//This method uses additive smoothing to account for values not found
						//In training data, zero probability is at key: "0"
						//If the set contains the feature, add its probability
						if (trainingMap.containsKey(strInt.getString())) {
							totalP = totalP + ( strInt.getValue() * Math.log(trainingMap.get(strInt.getString())));
						}else{
							totalP = totalP + (strInt.getValue() * PENALTY);
						}					

					}	//End of for each lemma


				} else {
					//No data for this profession, don't classify this one
					//This may be acceptable depending on the training data. Small training data sets
					//may not contain data for all professions
					totalP = Double.NEGATIVE_INFINITY;

				}

				//Add the log prior
				totalP = totalP + (Math.log(profession.getPrior()));

				//Check if this new profession is probable and save if so
				if (isGreater(topProbabilities, totalP)) {
					insertP(topProbabilities, topNames, totalP, profession.getName());
				}

			}	//End of for-each-profession


			String professions = "";
			for (int i = 0; i < OUTPUT_PROFESSION_NUMBER; i++) {
				String prof = topNames[i];

				//Write just labels
				professions = professions.concat(prof + ", ");
			}

			//Correct output according to assignment requirements
			professions = title.toString().concat(" : " + professions.substring(0, professions.length() - 2));
			context.write(new Text(professions), new Text(""));

		}	//End of map()


	}	//End of mapper class



	/**
	 * isGreater determines whether a value should be inserted into a given list
	 * by iteratively comparing it to each value in the list until a lesser value
	 * is found, or end of list.
	 * 
	 * @param list List of values to check
	 * @param value New value to compare to
	 * @return True if the new value is greater than at least one in the list
	 */
	private static boolean isGreater (double[] list, double value) {

		for (double d : list) {
			if (value > d) {
				return true;
			}
		}

		return false;
	}	//End of isGreater()


	/**
	 * This method inserts a new profession and probability into the array, maintaining 
	 * highest likelihood at the 0th position, and all others in descending order
	 * 
	 * @param dlist List of current best probabilities
	 * @param slist List of current best professions
	 * @param newd New probability
	 * @param news New Profession
	 */
	private static void insertP (double[] dlist, String[] slist, double newd, String news) {

		for (int i = 0; i < slist.length; i++) {
			//Check list from highest value [0] to end of list

			if (newd > dlist[i]) {
				//Found a place to insert, shift all other values right
				for (int x = slist.length - 1; x > i; x--) {
					dlist[x] = dlist[x - 1];
					slist[x] = slist[x - 1];
				}

				//Insert the new values and return
				dlist[i] = newd;
				slist[i] = news;
				return;
			}
		}

	}	//End of insertP

	/**
	 * This method reads the training data file and builds it into a map of maps, one for each 
	 * profession
	 * @param reader Reader opened on input file stream
	 * @return Map of profession data
	 * @throws IOException For any file reading error
	 * @throws RuntimeException If any data validation checks fail
	 */
	private static HashMap<String, Map<String, Double>> buildJobMapWithoutRFS(BufferedReader reader) 
			throws IOException {

		//Count number of lines read in for validation
		int lineCount = 0;

		//Map for all profession data to return
		HashMap<String, Map<String, Double>> outputMap = new HashMap<String, Map<String, Double>>(1000);

		//This loop builds each sub map for each profession
		while (reader.ready()) {

			String inputLine = reader.readLine();

			//Used for validating the map before returning
			lineCount++;

			//Split input line into title and data
			String[] splitLine = inputLine.split("\t");

			//Split data line and remove formatting/delimiter characters
			String[] tokens = splitLine[1].substring(1, splitLine[1].length() - 1).split(">,<");

			//Map to store data from this input line
			Map<String, Double> professionMap = new HashMap<String, Double>();

			//Put input data into map
			for (String tok : tokens) {
				String[] sd = tok.split(",");
				professionMap.put(sd[0], Double.parseDouble(sd[1]));
			}

			//Check that output map contains a key/value pair for each token in this person's file
			if (professionMap.size() != tokens.length) {
				throw new RuntimeException("Map contains " + professionMap.size() + " values, but "
						+ "exptected " + tokens.length + " based on tokens");
			}

			//Store this inner map in the outer map
			outputMap.put(splitLine[0], professionMap);

			//Verify that a sub map exists for each line (article data) read
			if ( lineCount != outputMap.size() )
				throw new RuntimeException("COLLISION DETECTED IN OUTPUT MAP DURING BUILDING OF MAP AT LABEL " + splitLine[0]);

		}	//Completed reading input file

		return outputMap;
	}	//end of buildJobMapWithoutRFS()



}	//End of Classifier
