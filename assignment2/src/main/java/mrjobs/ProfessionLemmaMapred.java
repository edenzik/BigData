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

import util.StringIntegerList;
import util.StringIntegerList.StringInteger;
import util.TitleProfessionParser;


/**
 * This class is used for Section C.2 of assignment 1. You are supposed to run
 * the code taking the lemma index filename as input, and output being the
 * inverted index.
 */
public class ProfessionLemmaMapred {
	public static class ProfessionLemmaMapper extends Mapper<Text, StringIntegerList, Text, StringIntegerList> {

		// Maps each title to a profession based on the input file
		public static Map<String, Set<String>> titleProfessionMap = new HashMap<String, Set<String>>();
		@Override
		protected void setup(Mapper<Text, StringIntegerList, Text, StringIntegerList>.Context context)
				throws IOException, InterruptedException {
			
			super.setup(context);
			//Builds a HashSet of people.txt article titles for filtering
			URI[] files = Job.getInstance(context.getConfiguration()).getCacheFiles();
			FileSystem fs = FileSystem.get(context.getConfiguration());
			BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(new Path(files[0]))));
			titleProfessionMap = TitleProfessionParser.buildTitleProfessionMap(reader);
		}

		@Override
		public void map(Text title, StringIntegerList lemmaCounts, Context context) throws IOException,
		InterruptedException {

			String index_str = indices.toString();
			String title = articleId.toString();
			Text word = new Text();
			StringInteger out;

			StringIntegerList temp = new StringIntegerList();
			temp.readFromString(index_str);

			List<StringInteger> list = temp.getIndices();

			for(StringInteger strInt : list){
				word.set(strInt.getString());
				out = new StringInteger(title, strInt.getValue());
				context.write(word, out);
			}
		}
	}

	public static class InvertedIndexReducer extends
	Reducer<Text, StringInteger, Text, StringIntegerList> {

		@Override
		public void reduce(Text lemma, Iterable<StringInteger> articlesAndFreqs, Context context)
				throws IOException, InterruptedException {

			ArrayList<StringInteger> list = new ArrayList<StringInteger>();
			for(StringInteger strInt : articlesAndFreqs){
				StringInteger temp = new StringInteger(strInt.getString(), strInt.getValue());
				list.add(temp);
			}

			StringIntegerList out = new StringIntegerList(list);
			context.write(lemma, out);

		}
	}

	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "invert");
		job.setJarByClass(ProfessionLemmaMapred.class);
		job.setMapperClass(ProfessionLemmaMapper.class);
		job.setReducerClass(InvertedIndexReducer.class);
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(StringInteger.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
