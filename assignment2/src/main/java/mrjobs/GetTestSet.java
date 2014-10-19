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
import util.TrainerHelper;
import java.io.FileReader;


/**
 * This class is used for Section C.2 of assignment 1. You are supposed to run
 * the code taking the lemma index filename as input, and output being the
 * inverted index.
 */
public class GetTestSet {

	private static final String HDFS_HOME = "hdfs://deerstalker.cs.brandeis.edu:54645/user/hadoop01/";
	// this is notably not final because it is possibly changed by a command line argument
	private static String training_path = HDFS_HOME + "resources/profession_train.txt";
	// String that is paired with the actual zero probability

	private static String data_path = "resources/profession_train.txt";

	public static class GetTestSetMapper extends Mapper<Text, Text, Text, Text> {

		// Maps each title to a profession based on the input file
		public static Map<String, Set<String>> titleProfessionMap = new HashMap<String, Set<String>>();
		@Override
		protected void setup(Mapper<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {

			super.setup(context);

			//Builds a map of people->profession
			URI[] files = Job.getInstance(context.getConfiguration()).getCacheFiles();
			FileSystem fs = FileSystem.get(context.getConfiguration());
			BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(new Path(files[0]))));
			//BufferedReader reader = new BufferedReader(new FileReader(data_path));
			
			titleProfessionMap = TitleProfessionParser.buildTitleProfessionMap(reader);
		}

		@Override
		public void map(Text title, Text lemmaCountsText, Context context) throws IOException,
		InterruptedException {
			Set<String> professions = titleProfessionMap.get(title.toString());
			if(professions == null) {
	
				context.write(title, lemmaCountsText);
				
			}
		}
	}

	
	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "test filter");

		// default assumes hard-coded training_data path
		if(args.length == 2) {
			// Adds profession training data as a cached file
			job.addCacheFile((new Path(training_path)).toUri());
		}
		// Optional third argument for training_data path
		else if(args.length == 3) {
			job.addCacheFile(new Path(HDFS_HOME + args[2]).toUri());
		}

		job.setJarByClass(GetTestSet.class);
		job.setMapperClass(GetTestSetMapper.class);
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setNumReduceTasks(0);
		// Map output types (though I'm not sure what the difference between these
		// and setOutputKey/SetOutputValue are...)
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
