import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 
 * In:
 * Title	<lemma, freq>
 * @author edenzik
 *
 */

public class WordCount {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text title, Text lemma_freq_pairs, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
        context.write(word, one);
      }
    }
    
    public ArrayList<AbstractMap<String, Integer>> splitter(String line){
    	String pair_pattern = "(<([^>]+),(\\d+)>)";
    	Pattern pattern = Pattern.compile(pair_pattern);
    	Matcher matcher = pattern.matcher(line);
    	
    	ArrayList<AbstractMap<String, Integer>> lemma_freq_pairs = new ArrayList<AbstractMap<String, Integer>>();
    	
    	AbstractMap<String, Integer> lemma_freq_pair = 
    	
    	String text    =
    	          "John writes about this, and John writes about that," +
    	                  " and John writes about everything. "
    	        ;

    	String patternString1 = "(John)";

    	Pattern pattern = Pattern.compile(patternString1);
    	Matcher matcher = pattern.matcher(text);

    	while(matcher.find()) {
    		String split_pair = matcher.group(1)
    		String lemma = matcher.group(1)[0];
    	   lemma_freq_pairs.add(new AbstractMap<String, Integer>(matcher.group(1));
    	}
    }
  }

  public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(WordCount.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path("/Users/edenzik/Downloads/alice_in_wonderland.txt"));
    FileOutputFormat.setOutputPath(job, new Path("/Users/edenzik/Downloads/alice_in_wonderland_oupt.txt"));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}