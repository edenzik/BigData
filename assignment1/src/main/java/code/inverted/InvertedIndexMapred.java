package code.inverted;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;

import util.StringIntegerList.StringInteger;
import util.StringIntegerList;


/**
 * This class is used for Section C.2 of assignment 1. You are supposed to run
 * the code taking the lemma index filename as input, and output being the
 * inverted index.
 */
public class InvertedIndexMapred {
	public static class InvertedIndexMapper extends Mapper<Text, Text, Text, StringInteger> {

		@Override
		public void map(Text articleId, Text indices, Context context) throws IOException,
				InterruptedException {
			// TODO: You should implement inverted index mapper here
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
			// TODO: You should implement inverted index reducer here
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
  		job.setJarByClass(InvertedIndexMapred.class);
    	job.setMapperClass(InvertedIndexMapper.class);
    	job.setReducerClass(InvertedIndexReducer.class);
    	//job.setNumReduceTasks(0);
    	job.setInputFormatClass(KeyValueTextInputFormat.class);
    	job.setOutputKeyClass(Text.class);
    	job.setOutputValueClass(StringInteger.class);
    	FileInputFormat.addInputPath(job, new Path(args[0]));
    	FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
