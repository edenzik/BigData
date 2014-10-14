package mrjobs;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import util.ProbHelper;
import util.StringDoubleList;
import util.StringDoubleList.StringDouble;
import util.StringIntegerList;
import util.StringIntegerList.StringInteger;
import util.TitleProfessionParser;


/**
 * This class is used for Section C.2 of assignment 1. You are supposed to run
 * the code taking the lemma index filename as input, and output being the
 * inverted index.
 */
public class Trainer {
	
    private static String training_path = "hdfs://deerstalker.cs.brandeis.edu:54645/user/hadoop01/resources/profession_train.txt";

	public static class TrainerMapper extends Mapper<Text, Text, Text, StringIntegerList> {

		// Maps each title to a profession based on the input file
		public static Map<String, Set<String>> titleProfessionMap = new HashMap<String, Set<String>>();
		@SuppressWarnings("deprecation")
		@Override
		protected void setup(Mapper<Text, Text, Text, StringIntegerList>.Context context)
				throws IOException, InterruptedException {

			super.setup(context);
			JobConf job = new JobConf();
			try {
				//DistributedCache.addCacheFile(new URI("/resources/profession_train.txt#profession_train.txt"), job);
				//DistributedCache.addCacheFile(new URI(training_path), job);
				DistributedCache.addCacheFile(new URI("hdfs://user/hadoop01/resources/profession_train.txt"), job);
			} catch(URISyntaxException e) {
				e.printStackTrace();
			}
			
			//Builds a map of people->profession
			Path[] files = Job.getInstance(context.getConfiguration()).getLocalCacheFiles();
			FileSystem fs = FileSystem.get(context.getConfiguration());
			BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(files[0])));
			titleProfessionMap = TitleProfessionParser.buildTitleProfessionMap(reader);
		}

		@Override
		public void map(Text title, Text lemmaCountsText, Context context) throws IOException,
		InterruptedException {

			StringIntegerList temp = new StringIntegerList();
			temp.readFromString(lemmaCountsText.toString());
			Set<String> professions = titleProfessionMap.get(title.toString());
			for(String s: professions) {
				context.write(new Text(s), temp);
			}
		}
	}

	public static class TrainerReducer extends
	Reducer<Text, StringIntegerList, Text, StringDoubleList> {

		@Override
		public void reduce(Text lemma, Iterable<StringIntegerList> lemmaFreqIter, Context context)
				throws IOException, InterruptedException {
			// merge all StringIntegerLists for a given profession s.t. each lemma
			// has only one entry in the StringIntegerList
			Map<String, Double> lemmaFreqMap = ProbHelper.getAggregateMap(lemmaFreqIter);
			// sum up all frequencies into denominators
			double denominator = ProbHelper.getFrequencySum(lemmaFreqMap);
			//TODO: check for overflow
			
			// Map each lemma to (total # of occurences / total # of occurences of all lemmas)
			// for a given profession
			List<StringDouble> list = new ArrayList<StringDouble>();
			for(String s : lemmaFreqMap.keySet()) {
				list.add(new StringDouble(s, lemmaFreqMap.get(s) / denominator));
			}

			StringDoubleList out = new StringDoubleList(list);
			context.write(lemma, out);
		}
	}

	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "trainer");
		job.setJarByClass(Trainer.class);
		job.setMapperClass(TrainerMapper.class);
		job.setReducerClass(TrainerReducer.class);
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(StringDoubleList.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
