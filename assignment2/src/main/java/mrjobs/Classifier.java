package mrjobs;



import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import util.Profession;
import util.StringDoubleList;
import util.StringIntegerList;
import util.StringIntegerList.StringInteger;


/**
 * This class is used to classify articles passed in based on training previously done
 */
public class Classifier {
	
	private static String DEFAULT_TRAINING_PATH = "resources/training";
	private static int OUTPUT_PROFESSION_NUMBER = 3;
	
    public static class ClassifyMapper extends Mapper<Text, StringIntegerList, Text, StringIntegerList> {

        @Override
        public void map(Text title, StringIntegerList lemmalists, Context context) throws IOException, InterruptedException {
        
            	context.write(title, lemmalists);
            	
        }
    }

    public static class ClassifyReducer extends Reducer<Text, StringIntegerList, Text, Text> {

//        @Override
        public void reduce(Text title, StringIntegerList lemmafreq, Context context)
                throws IOException, InterruptedException {
    
        	//These hold the top (so far) 3 professions and probabilities for this person
        	String[] topNames = new String[OUTPUT_PROFESSION_NUMBER];
        	double[] topProbabilities = new double[OUTPUT_PROFESSION_NUMBER];
        	
        	//Set topProbabilities to negative infinity
        	//This way any new value will be greater
        	for (int i = 0; i < topProbabilities.length; i++) {
        		topProbabilities[i] = Double.NEGATIVE_INFINITY;
        	}
        	
        	Path trainingData = new Path(context.getCacheFiles()[0]);
        	
        	
        	//Loop through each possible profession, and calculate the probability for this person
        	for (Profession profession : Profession.values()) {
        		
        		double totalP = 0;
        		
        		//Build/get a list of lemma-freq for this profession
        		Map<String, Double> trainingMap = getTrainingMap(profession.getName(), trainingData);
        		
        		//StringIntegerList is not iterable, so turn it into an iterable object
        		List<StringInteger> lemmalist = lemmafreq.getIndices();
        		
        		
        		//For each lemma in this list, add the probability 
        		for (StringInteger stInt : lemmalist) {
        			
        			//This method ignores words that don't appear in the training data
        			//If we apply smoothing, this will need to be changed
        			if (trainingMap.containsKey(stInt.getString())) {
						totalP += stInt.getValue()
								* trainingMap.get(stInt.getString());
					}
        		}
        		
        		
        			
        		//Multiply by the prior
        		totalP = totalP * profession.getPrior();
        		
        		
        		//Check if this new profession is probable and save if so
        		if (isGreater(topProbabilities, totalP)) {
        			insertP(topProbabilities, topNames, totalP, profession.getName());
        		}
        		
        		
        		
        	}	//End of for-each-profession
        	
        	
        	
        	String professions = "";
        	for (String prof : topNames) {
        		professions = professions.concat(prof);
        	}
            context.write(title, new Text(professions));

        }


    }
    
    /**
     * Classifier uses training data to classify people
     * input: Lemma Index of format <Title, <Lemma, Freq> > from assignment 1
     * output: Profession Index of format <Title, <Profession 1, Profession 2, Profession 3> >
     * Note: Output will be in Text, Text format.
     * Note: Optional third argument is training data. If left blank, internal field will be used.
     * 
     * @param args args[0] is input directory, args[1] is output directory, (optional) args[2] is training data
     * @throws Exception for any exception
     */
    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "classify");
        job.setJarByClass(Classifier.class);
        job.setMapperClass(ClassifyMapper.class);
        job.setReducerClass(ClassifyReducer.class);
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        //Allows passing training data reference from command line
        if(args.length > 2){

            job.addCacheFile(Paths.get(args[2]).toUri());
        
        //If no command line reference specified, use standard one
        } else {

            job.addCacheFile(Paths.get(DEFAULT_TRAINING_PATH).toUri());
 
        }
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }	//End of main()

    
    /**
     * isGreater determines whether a value should be inserted into a given list
     * by iteratively comparing it to each value in the list until a lesser value
     * is found, or end of list.
     * 
     * @param list List of values to check
     * @param value New value to compare to
     * @return True if the new value is greater than at least one in the list
     */
    private static boolean isGreater (double[] list, double value) {
    	
    	for (double d : list) {
    		if (value > d) {
    			return true;
    		}
    	}
    	
    	return false;
    }	//End of isGreater()
    
    
    /**
     * This method inserts a new profession and probability into the array, maintaining 
     * highest likelihood at the 0th position, and all others in descending order
     * 
     * @param dlist List of current best probabilities
     * @param slist List of current best professions
     * @param newd New probability
     * @param news New Profession
     */
    private static void insertP (double[] dlist, String[] slist, double newd, String news) {
    	
    	for (int i = 0; i < slist.length; i++) {
    		//Check list from highest value [0] to end of list
    		
    		if (newd > dlist[i]) {
    			//Found a place to insert, shift all other values right
    			for (int x = slist.length - 1; x > i; x--) {
    				dlist[x] = dlist[x - 1];
    				slist[x] = slist[x - 1];
    			}
    			
    			//Insert the new values and return
    			dlist[i] = newd;
    			slist[i] = news;
    			return;
    		}
    	}
    	
    	
    	
    }	//End of insertP
    
	private static Map<String, Double> getTrainingMap(String profession, Path dataPath) throws IOException {
		
		BufferedReader reader = new BufferedReader(new FileReader(dataPath.toString()));
		
		while (reader.ready()) {
			
			String inputLine = reader.readLine();
			
			if (inputLine.startsWith(profession)) {
				
				//Found the profession we are looking for
				StringDoubleList list = new StringDoubleList();
				list.readFromString(inputLine.split("\t")[1].trim());
				
				reader.close();
				
				return list.getMap();
			}
			
		}
		
		reader.close();
		return null;
	}


}	//End of Classifier
