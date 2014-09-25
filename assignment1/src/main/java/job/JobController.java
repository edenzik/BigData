package job;

import code.articles.GetArticlesMapred;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;

import edu.umd.cloud9.collection.wikipedia.WikipediaPage;
import util.WikipediaPageInputFormat;



public class JobController extends Configured implements Tool{

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf1 = new Configuration();
    	Job job1 = Job.getInstance(conf1, "get articles");
    	job1.addCacheFile(GetArticlesMapred.class.getResource("/code/articles/data/people.txt").toURI());
  		job1.setJarByClass(getClass());
    	job1.setMapperClass(GetArticlesMapred.GetArticlesMapper.class);
    	job1.setNumReduceTasks(0);
    	job1.setInputFormatClass(WikipediaPageInputFormat.class);
    	job1.setOutputKeyClass(Text.class);
    	job1.setOutputValueClass(Text.class);
    	FileInputFormat.addInputPath(job1, new Path(args[0]));
    	FileOutputFormat.setOutputPath(job1, new Path(args[1]));
    	
    	if (job1.waitForCompletion(true)){
	    	Configuration conf2 = new Configuration();
    		Job job2 = Job.getInstance(conf2, "repeat");
  			job2.setJarByClass(getClass());
    		job2.setMapperClass(GetArticlesMapred.SecondMapper.class);
    		job2.setNumReduceTasks(0);
    		job2.setInputFormatClass(KeyValueTextInputFormat.class);
    		job2.setOutputKeyClass(Text.class);
    		job2.setOutputValueClass(Text.class);
    		FileInputFormat.addInputPath(job2, new Path(args[1]));
    		FileOutputFormat.setOutputPath(job2, new Path(args[2]));

	    	return job2.waitForCompletion(true) ? 0 : 1;
	    } else {
	    	return 1;
	    }
	}

	public static void main(String[] args) throws Exception {
		// TODO: you should implement the Job Configuration and Job call
		// here
		int rc = ToolRunner.run(new JobController(), args);
		System.exit(rc);
	
	}
}