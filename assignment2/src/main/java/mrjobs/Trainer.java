package mrjobs;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import util.StringDoubleList;
import util.StringDoubleList.StringDouble;
import util.StringIntegerList;
import util.StringIntegerList.StringInteger;
import util.TitleProfessionParser;


/**
 * This class is used for Section 1 of Assignment 2 to perform the training task on the lemma index.
 */
public class Trainer {

	// HDFS home directory
	private static final String HDFS_HOME = "hdfs://deerstalker.cs.brandeis.edu:54645/user/hadoop01/";
	// this is notably NOT final because it is possibly changed by a command line argument
	private static String training_path = HDFS_HOME + "resources/profession_train.txt";
	// String that is paired with the actual zero probability
	public static final String ZERO_PROBABILITY_STRING = "ZERO";
	// Numerator of probability of 0 probability for additive smoothing
	private static final int ALPHA = 1;

	// The actual map task of taking a our index of people -> {lemma -> freq}
	// and converting it into an index of profession -> {lemma -> freq}
	public static class TrainerMapper extends Mapper<Text, Text, Text, StringIntegerList> {

		// Maps each title to a profession based on the input file
		public static Map<String, Set<String>> titleProfessionMap = new HashMap<String, Set<String>>();

		@Override
		protected void setup(Mapper<Text, Text, Text, StringIntegerList>.Context context)
				throws IOException, InterruptedException {

			super.setup(context);

			//Builds a map of people->profession
			URI[] files = Job.getInstance(context.getConfiguration()).getCacheFiles();
			FileSystem fs = FileSystem.get(context.getConfiguration());
			BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(new Path(files[0]))));

			titleProfessionMap = TitleProfessionParser.buildTitleProfessionMap(reader);
		}

		@Override
		public void map(Text title, Text lemmaCountsText, Context context) throws IOException,
		InterruptedException {
			StringIntegerList lemmaCounts = new StringIntegerList();
			lemmaCounts.readFromString(lemmaCountsText.toString());
			Set<String> professions = titleProfessionMap.get(title.toString());
			if(professions != null) {
				for(String s: professions) {
					context.write(new Text(s.toLowerCase().trim()), lemmaCounts);
				}
			}
		}
	}

	// The reducer task takes an index of profession -> {lemma, freq} and generates
	// a binomial probability model whereby the probability for a given lemma is
	// the (# of articles that the lemma appears in) / (total # of articles).
	// The output is of the format profession -> {lemma, probability}
	public static class TrainerReducer extends
	Reducer<Text, StringIntegerList, Text, StringDoubleList> {

		@Override
		public void reduce(Text profession, Iterable<StringIntegerList> lemmaFreqIter, Context context)
				throws IOException, InterruptedException {
			
			//Bernouli Model
			Map<String, Double> freqMap = new HashMap<String, Double>();
			int articleCount = 0;
			for(StringIntegerList l: lemmaFreqIter) {
				articleCount++;
				List<StringInteger> list = l.getIndices();
				for(StringInteger i: list) {
					String key = i.getString();
					if(freqMap.get(key) == null) {
						freqMap.put(key, 1.0);
					} else {
						freqMap.put(key, freqMap.get(key) + 1.0);
					}
				}
			}

			// Map each lemma to (total # of occurences / total # of occurences of all lemmas)
			// for a given profession
			List<StringDouble> list = new ArrayList<StringDouble>();

			//Check on bernouli smoothing
			double denominator = articleCount + ALPHA;
			
			for(String s : freqMap.keySet()) {
				double numerator = freqMap.get(s) + ALPHA;
				double probability = numerator / denominator;
				list.add(new StringDouble(s, probability));
			}

			StringDoubleList out = new StringDoubleList(list);
			context.write(profession, out);
		}
	}

	// Main method to handle normal hadoop setup, and to also ensure
	// that the necessary training data is cached properly on HDFS
	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "trainer");

		// default assumes hard-coded training_data path
		if(args.length == 2) {
			// Adds profession training data as a cached file
			job.addCacheFile((new Path(training_path)).toUri());
		}
		// Optional third argument for training_data path
		else if(args.length == 3) {
			job.addCacheFile(new Path(HDFS_HOME + args[2]).toUri());
		}

		job.setJarByClass(Trainer.class);
		job.setMapperClass(TrainerMapper.class);
		job.setReducerClass(TrainerReducer.class);
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		// Map output types (though I'm not sure what the difference between these
		// and setOutputKey/SetOutputValue are...)
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(StringIntegerList.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
