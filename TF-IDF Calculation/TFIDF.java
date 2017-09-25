/*
 Name: Harish Pendyala
 Student ID# 800956847
 */
package org.myorg;

import java.io.IOException;
import java.util.regex.Pattern;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import org.apache.hadoop.fs.FileSystem;

//Create a class with name TFIDF
public class TFIDF extends Configured implements Tool {
    private static final Logger LOG = Logger.getLogger(TFIDF.class);

    public static void main(String[] args) throws Exception {
    	//Execute the TFIDF using chaining( calling TermFrequency by instantiating it)
    	// execute the TermFrequency job
    	int abc = ToolRunner.run(new TermFrequency(),args);
    	// execute the TFIDF if TermFrequency execution completes successfully
    	if (abc == 0){
    		
    		int res = ToolRunner.run(new TFIDF(), args);
            System.exit(res);
    	} else{
    		 
    	}

    }


    public int run(String[] args) throws Exception {
        //Pass the total number of files count
        Configuration config = getConf();
        FileSystem FS = FileSystem.get(config);
        final int no_of_files = FS.listStatus(new Path(args[0])).length;
        config.setInt("no_of_files", no_of_files);

        //  create a job to execute TFIDF
        Job job = Job.getInstance(getConf(), "tfidf");
        job.setJarByClass(this.getClass());
        // Get the input from the destination provided in args[1] and write the output to the destination provided in args[2]
        FileInputFormat.addInputPaths(job, args[1]);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        // Assign the Map and Reduce classes
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        // set input and output type
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

 
    // Mapper for TFIDF 
    public static class Map extends Mapper<LongWritable, Text, Text, Text> {
        private Text word = new Text();


        public void map(LongWritable offset, Text lineText, Context context)
                throws IOException, InterruptedException {
            String line = lineText.toString();

            String final_value = "";
            // Split each line based on the delimiter(#####) and tab
            String term = line.split("#####")[0];
            String file_tf = line.split("#####")[1];
            String file_name = file_tf.split("\\t")[0];
            String tf = file_tf.split("\\t")[1];
            // format the output of Map Phase
            final_value = file_name+"="+tf;
            // write the output
            context.write(new Text(term), new Text(final_value));
                  
            
        }
    }

    // Reducer for TFIDF
    public static class Reduce extends Reducer<Text, Text, Text, DoubleWritable> {
            
        @Override
        public void reduce(Text word, Iterable<Text> inputs, Context context)
                throws IOException, InterruptedException {
            double idf = 0.0;
            double tfidf = 0.0;
            String key = "";
            long no_of_files = context.getConfiguration().getInt("no_of_files", 0);
            
            // Use the HashMap to store in the key, Value format
            HashMap<String, Double> file_tfidf = new HashMap<String, Double>();

            // use the ArrayList to store the unique filenames(easy to verify if exists already)
            ArrayList<String> al = new ArrayList<String>();
            String file_name = null;
            // Read the input and split based on the delimiter(#####) and '+'
            for (Text file : inputs) {
                String file_value = file.toString();
                
                key = word.toString() + "#####" + file_value.substring(0, file_value.indexOf("="));
                file_name = file_value.substring(0, file_value.indexOf("="));
                // retrieve the TF value and convert to Double
                double tf = Double.parseDouble(file_value.substring(file_value.indexOf("=") + 1, file_value.length()));
                //Store the key, value pairs
                file_tfidf.put(key, tf);
                //store the filename if it is unique for the word
                if (al.contains(file_name)){
                	
                } else{
                	al.add(file_name);
                }
                
                
            }
            // calculate idf using the given formula log(1+ no.of files/total number of files in which word exists
            int count = al.size();
            idf = Math.log10(1 + (no_of_files / count));

            // calculate TFIDF = TF*IDF
            for (String term : file_tfidf.keySet()) {
                tfidf = file_tfidf.get(term) * idf;

                // write the tfidf values for each term
                context.write(new Text(term), new DoubleWritable(tfidf));
            }
        }
    }
}