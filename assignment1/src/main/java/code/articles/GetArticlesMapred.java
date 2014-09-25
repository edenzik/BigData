package code.articles;

import java.io.IOException;
import java.io.File;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.HashSet;
import java.util.Map;
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
import util.StringIntegerList;

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
	public static class GetArticlesMapper extends Mapper<LongWritable, WikipediaPage, Text, StringIntegerList> {
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
			if(peopleArticlesTitles.contains(inputPage.title())){
				Text title = new Text();
				title.set(inputPage.getTitle());
				StringIntegerList indices = 
					new StringIntegerList(TokenizeLemmatize.parse(inputPage.content()));
				context.write(title, indices);
			}
		}
	}

	public static class SecondMapper extends Mapper<Text, Text, Text, Text> {
		public static Set<String> peopleArticlesTitles = new HashSet<String>();


		@Override
		public void map(Text title, Text content, Context context)
				throws IOException, InterruptedException {
			// TODO: You should implement getting article mapper here
				Text works = new Text();
				works.set("IT WORKS BITCH");
				context.write(works, content);
			
		}
	}
}
