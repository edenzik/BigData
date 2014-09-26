package code.articles;

import java.io.IOException;
import java.io.File;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.HashSet;
import java.util.Set;
import java.net.URI;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import edu.umd.cloud9.collection.wikipedia.WikipediaPage;
import util.WikipediaPageInputFormat;

/**
 * This class is used for Section A of assignment 1. You are supposed to
 * implement a main method that has first argument to be the dump wikipedia
 * input filename , and second argument being an output filename that only
 * contains articles of people as mentioned in the people auxiliary file.
 */
public class GetArticlesMapred {

	private static String people_path = "/home/hadoop01/people.txt";
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
	public static class GetArticlesMapper extends Mapper<LongWritable, WikipediaPage, Text, Text> {
		public static Set<String> peopleArticlesTitles = new HashSet<String>();

		@Override
		protected void setup(Mapper<LongWritable, WikipediaPage, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO: You should implement people articles load from
			// DistributedCache here
			super.setup(context);
			URI[] localPaths = context.getCacheFiles();
			File f = new File(localPaths[0].getPath()); //LOOK ME UP
			BufferedReader reader = new BufferedReader(new FileReader(f));
			String name;
			while((name = reader.readLine()) != null){
				peopleArticlesTitles.add(name);
			}

			

		}

		@Override
		public void map(LongWritable offset, WikipediaPage inputPage, Context context)
				throws IOException, InterruptedException {
			// TODO: You should implement getting article mapper here
			Text title = new Text();
			Text body = new Text();		
			if(peopleArticlesTitles.contains(inputPage.getTitle())){
				title.set(inputPage.getTitle());
				body.set(inputPage.getContent());
				context.write(title, body);
			}
		}
	}

	public static void main(String[] args) {
		// TODO: you should implement the Job Configuration and Job call
		// here
		try{
			Configuration conf = new Configuration();
    		Job job = Job.getInstance(conf, "get articles");
    		job.addCacheFile(new Path(args[2]).toUri());
  		  	job.setJarByClass(GetArticlesMapred.class);
    		job.setMapperClass(GetArticlesMapper.class);
    		job.setNumReduceTasks(0);
    		job.setInputFormatClass(WikipediaPageInputFormat.class);
    		job.setOutputKeyClass(LongWritable.class);
    		job.setOutputValueClass(WikipediaPage.class);
    		FileInputFormat.addInputPath(job, new Path(args[0]));
    		FileOutputFormat.setOutputPath(job, new Path(args[1]));
    		System.exit(job.waitForCompletion(true) ? 0 : 1);
    	} catch(Exception e){
    		e.printStackTrace();
    	}
	}
}
