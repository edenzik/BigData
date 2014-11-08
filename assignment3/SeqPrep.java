import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.io.BufferedReader;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.NamedVector;
import org.apache.mahout.math.VectorWritable;
import java.io.InputStreamReader;

public class SeqPrep {

    public static void main(String args[]) throws IOException{


        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path outPath = new Path("filtered_seq/");
        Path inPath = new Path(
            "hdfs://deerstalker.cs.brandeis.edu:54645/user/hadoop01/input/filteredinput_filtered_pa3_random.txt");

        BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(inPath)));

        List<NamedVector> reviews  = new ArrayList<NamedVector>();
        int count = 0;
        while(reader.hasNext()){
            String line = reader.nextLine();
            reviews.add((new Vector<String>(Arrays.asList(line.split()))), "" + count);
            count++;
        }

        SequenceFile.Writer writer = new SequenceFile.Writer(fs,  conf, outPath, Text.class, VectorWritable.class);

        VectorWritable vec = new VectorWritable();
        for(NamedVector vector : reviews){
            vec.set(vector);
            writer.append(new Text(vector.getName()), vec);
        }
        writer.close();

        SequenceFile.Reader reader = new SequenceFile.Reader(fs, outPath, conf);

        Text key = new Text();
        VectorWritable value = new VectorWritable();
        while(reader.next(key, value)){
            System.out.println(key.toString() + " , " + value.get().asFormatString());
        }
        reader.close();

    }

}