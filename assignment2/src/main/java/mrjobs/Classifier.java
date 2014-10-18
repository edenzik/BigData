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
import util.StringDoubleList;
import util.StringIntegerList;
import util.StringIntegerList.StringInteger;


/**
 * This class is used to classify articles passed in based on training previously done
 */
public class Classifier {

	//String name of the ZERO probability key for lookup
	private static final String ZERO_KEY = Trainer.ZERO_PROBABILITY_STRING;
	
	private static String DEFAULT_TRAINING_PATH = "hdfs://deerstalker.cs.brandeis.edu:54645/user/hadoop01/output/old_training/part-r-00000";
	private static int OUTPUT_PROFESSION_NUMBER = 10;


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
		job.setNumReduceTasks(0);
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		//Allows passing training data reference from command line
		if(args.length > 2){

			job.addCacheFile(new Path(args[2]).toUri());

			//If no command line reference specified, use standard one
		} else {

			job.addCacheFile(new Path(DEFAULT_TRAINING_PATH).toUri());

		}

		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}	//End of main()


	public static class ClassifyMapper extends Mapper<Text, Text, Text, Text> {

		private HashMap<String, Map<String, Double>> fullProfessionMap = new HashMap<String, Map<String, Double>>();

		@Override
		protected void setup(Mapper<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {

			super.setup(context);

			//Builds a map of people->profession
			URI[] files = Job.getInstance(context.getConfiguration()).getCacheFiles();
			FileSystem fs = FileSystem.get(context.getConfiguration());
			System.out.println(new Path(files[0]));
			BufferedReader reader = new BufferedReader( new InputStreamReader( fs.open(new Path(files[0])) ) );
			fullProfessionMap = buildJobMapWithoutRFS(reader);
//			fullProfessionMap = buildJobMap(reader);
			reader.close();
		}

		//        @Override
		public void map(Text title, Text listText, Context context)
				throws IOException, InterruptedException {

			StringIntegerList lemmaFreq = new StringIntegerList();

			lemmaFreq.readFromString(listText.toString());

			//These hold the top (so far) 3 professions and probabilities for this person
			String[] topNames = new String[OUTPUT_PROFESSION_NUMBER];
			double[] topProbabilities = new double[OUTPUT_PROFESSION_NUMBER];

			//Set topProbabilities to negative infinity
			//This way any new value will be greater
			for (int i = 0; i < topProbabilities.length; i++) {
				topProbabilities[i] = Double.NEGATIVE_INFINITY;
			}



			//Loop through each possible profession, and calculate the probability for this person
			for (Profession profession : Profession.values()) {

				double totalP = 0;
				Map<String, Double> trainingMap = fullProfessionMap.get(profession.getName());


				//Did we have data for this profession in training data?
				if (trainingMap != null) {

					//StringIntegerList is not iterable, so turn it into an iterable object
					List<StringInteger> lemmaList = lemmaFreq.getIndices();


					//For each lemma in this list, add the probability 
					for (StringInteger stInt : lemmaList) {

						//This method uses additive smoothing to account for values not found
						//In training data, zero probability is at key: "0"
						if (trainingMap.containsKey(stInt.getString())) {
							totalP = totalP + ( stInt.getValue() * Math.log(trainingMap.get(stInt.getString())) );

//						} else if (false && trainingMap.get(ZERO_KEY) == null){
//							throw new RuntimeException("NO zero found. Printing map for " + profession.getName() + " : \n" + trainingMap.toString());
//							
//							
//							
//							
						} else {
							double zeroP;
							if (trainingMap.get(ZERO_KEY) != null) {
								zeroP = trainingMap.get(ZERO_KEY);
							} else {
								throw new RuntimeException("NO zero found. Printing map for " + profession.getName() + " : \n" + trainingMap.toString());
							}
							//No match, use the zero probability
							totalP = totalP + ( stInt.getValue() * Math.log(zeroP) );
						}


					}	//End of for each lemma


				} else {
					//No data for this profession
					//Don't match this one
					totalP = Double.NEGATIVE_INFINITY;
				}





				//Add the log prior
				totalP = totalP + Math.log(profession.getPrior());


				//Check if this new profession is probable and save if so
				if (isGreater(topProbabilities, totalP)) {
					insertP(topProbabilities, topNames, totalP, profession.getName());
				}



			}	//End of for-each-profession



			String professions = "";
			for (int i = 0; i < OUTPUT_PROFESSION_NUMBER; i++) {
				String prof = topNames[i];
				double total = topProbabilities[i];
				professions = professions.concat(prof + "(" + total + "), ");
			}
			professions = professions.substring(0, professions.length() - 2);
			context.write(title, new Text(professions));

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

	private static HashMap<String, Map<String, Double>> buildJobMap(BufferedReader reader) throws IOException {
		int lineCount = 0;


		HashMap<String, Map<String, Double>> outputMap = new HashMap<String, Map<String, Double>>(1000);

		//This loop builds each sub map for each profession
		while (reader.ready()) {


			String inputLine = reader.readLine();
			lineCount++;

			String[] splitLine = inputLine.split("\t");

			StringDoubleList list = new StringDoubleList();

			int linesRead = list.readFromString(splitLine[1]);
			
			if (! (list.toString().contains(ZERO_KEY)) )
					throw new RuntimeException("No ZERO found in list for " + splitLine[0] + ":\n"
							+ "length of input string is " + splitLine[1].length() + "\n"
							+ "This is line number " + lineCount + "\n"
							+ "readFromString returned a count of " + linesRead + "\n"
							+ "List has " + list.getIndices().size() + " elements\n"
							+ list.toString());

			outputMap.put(splitLine[0], list.getMap());
			
			if ( !list.getMap().toString().contains(ZERO_KEY) )
				throw new RuntimeException("No ZERO found in map text for " + splitLine[0] + ":\n" + list.getMap().toString());
			if ( !list.getMap().containsKey(ZERO_KEY) )
				throw new RuntimeException("No ZERO found in map keys for " + splitLine[0] + ":\n" + list.getMap().toString());

			if ( !(lineCount == outputMap.size()) )
				throw new RuntimeException("COLLISION DETECTED IN OUTPUT MAP DURING BUILDING OF MAP AT LABEL " + splitLine[0]);
		}

//		if (false)
//			throw new RuntimeException("I have maps for: " + outputMap.keySet().toString());
//		if (false) {
//			throw new RuntimeException("Read " + lineCount + " lines from input file and built " + outputMap.size() + " maps.");
//		}

		return outputMap;
	}
	
	private static HashMap<String, Map<String, Double>> buildJobMapWithoutRFS(BufferedReader reader) throws IOException {
		int lineCount = 0;


		HashMap<String, Map<String, Double>> outputMap = new HashMap<String, Map<String, Double>>(1000);

		//This loop builds each sub map for each profession
		while (reader.ready()) {


			String inputLine = reader.readLine();
			lineCount++;

			String[] splitLine = inputLine.split("\t");

			StringDoubleList list = new StringDoubleList();

			String[] tokens = splitLine[1].substring(1, splitLine[1].length() - 1).split(">,<");
			Map<String, Double> professionMap = new HashMap<String, Double>();
			
			for (String tok : tokens) {
				String[] sd = tok.split(",");
				professionMap.put(sd[0], Double.parseDouble(sd[1]));
			}
			
			if (professionMap.size() != tokens.length) {
				throw new RuntimeException("Map contains " + professionMap.size() + " values, but exptected " + tokens.length + " based on tokens");
			}
			
			if ( !professionMap.toString().contains(ZERO_KEY) )
				throw new RuntimeException("No ZERO found in map text for " + splitLine[0] + ":\n"
						+ "Map has " + professionMap.size() + " entries.\n"
						+ professionMap.toString());
			if ( !professionMap.containsKey(ZERO_KEY) )
				throw new RuntimeException("No ZERO found in map keys for " + splitLine[0] + ":\n"
						+ "Map has " + professionMap.size() + " entries.\n"
						+ professionMap.toString());
			
			
			outputMap.put(splitLine[0], professionMap);
			
			if ( lineCount != outputMap.size() )
				throw new RuntimeException("COLLISION DETECTED IN OUTPUT MAP DURING BUILDING OF MAP AT LABEL " + splitLine[0]);
		}

//		if (false)
//			throw new RuntimeException("I have maps for: " + outputMap.keySet().toString());
//		if (false) {
//			throw new RuntimeException("Read " + lineCount + " lines from input file and built " + outputMap.size() + " maps.");
//		}

		return outputMap;
	}



}	//End of Classifier
