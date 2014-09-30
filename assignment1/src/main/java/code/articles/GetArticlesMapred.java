package code.articles;

import java.io.*;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.net.URI;
//import java.io.file.Paths;
import java.nio.*;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import edu.umd.cloud9.collection.wikipedia.WikipediaPage;
import util.WikipediaPageInputFormat;
import util.StringIntegerList;
import util.TokenizeLemmatize;

/**
 * This class is used for Section A of assignment 1. You are supposed to
 * implement a main method that has first argument to be the dump wikipedia
 * input filename , and second argument being an output filename that only
 * contains articles of people as mentioned in the people auxiliary file.
 */
public class GetArticlesMapred {

	private static String people_path = "hdfs://deerstalker.cs.brandeis.edu:54645/user/hadoop01/resources/people.txt";
	//@formatter:off
	/**
	 * Input:
	 * 		Page offset 	WikipediaPage
	 * Output
	 * 		Page offset 	WikipediaPage
	 * @author Tuan
	 *
	 */
	//@formatter:on
	public static class GetArticlesMapper extends Mapper<LongWritable, WikipediaPage, Text, StringIntegerList> {
		public static Set<String> peopleArticlesTitles = new HashSet<String>();

		@Override
		protected void setup(Mapper<LongWritable, WikipediaPage, Text, StringIntegerList>.Context context)
				throws IOException, InterruptedException {
			// TODO: You should implement people articles load from
			// DistributedCache here
			super.setup(context);
		
            FileSystem fs = FileSystem.get(context.getConfiguration());
			BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(new Path(people_path))));
			String name;
			while((name = reader.readLine()) != null){
				peopleArticlesTitles.add(name);
			}
		}

		@Override
		public void map(LongWritable offset, WikipediaPage inputPage, Context context)
				throws IOException, InterruptedException {
			// TODO: You should implement getting article mapper here	
			if(peopleArticlesTitles.contains(inputPage.getTitle())){
				Text title = new Text();
				title.set(inputPage.getTitle());
				StringIntegerList indices = 
					new StringIntegerList(TokenizeLemmatize.parse(inputPage.getContent()));
				context.write(title, indices);
			}
		}
	}
	
	public static void main(String[] args) throws Exception{

		Configuration conf = new Configuration();
    	Job job = Job.getInstance(conf, "get articles");

  		job.setJarByClass(GetArticlesMapred.class);
    	job.setMapperClass(GetArticlesMapper.class);
    	job.setNumReduceTasks(0);
    	job.setInputFormatClass(WikipediaPageInputFormat.class);
    	job.setOutputKeyClass(Text.class);
    	job.setOutputValueClass(StringIntegerList.class);
    	
    	if(args.length > 2){
    		people_path = args[2];
    	} else {
    		people_path = "hdfs://deerstalker.cs.brandeis.edu:54645/user/hadoop01/resources/people.txt";
    	}

    	FileInputFormat.addInputPath(job, new Path(args[0]));
    	FileOutputFormat.setOutputPath(job, new Path(args[1]));
    	System.exit(job.waitForCompletion(true) ? 0 : 1);

	}
}
