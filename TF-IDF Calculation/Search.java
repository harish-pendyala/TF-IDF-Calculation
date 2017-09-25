/*
 Name: Harish Pendyala
 Student ID# 800956847
 */
package org.myorg;

import java.io.IOException;
import java.util.regex.Pattern;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;

//Create a class with name Search
public class Search extends Configured implements Tool {
    private static final Logger LOG = Logger.getLogger(Search.class);

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Search(), args);
        System.exit(res);
    }

    // create a hadoop chain job with 2 jobs with different Map and Reduce for each job

    public int run(String[] args) throws Exception {
        // create a job to execute Search
    	// pass the search key word to mapper present in args[2]
        final String key_word = args[2];
        
        getConf().set("key_word", key_word);
        Job job = Job.getInstance(getConf(), "search");
        job.setJarByClass(this.getClass());


           	
        // Get the input from the destination provided in args[0] and write the output to the destination provided in args[1]
        FileInputFormat.addInputPaths(job, args[0]);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(Map.class);;
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }


    //Mapper for Serach
    public static class Map extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable offset, Text lineText, Context context)
                throws IOException, InterruptedException {
            String line = lineText.toString();
            Text currentWord = new Text();
            String search = context.getConfiguration().get("key_word");
            //Convert the search key words to lowercase
            search = search.toLowerCase();
            // split the key word using spaces
            String[] words = search.split(" ");
            // Split each line based on the delimiter(#####) and tab
            String term = line.split("#####")[0];
            String file_tfidf = line.split("#####")[1];
            String file_name = file_tfidf.split("\\t")[0];
            String tfidf = file_tfidf.split("\\t")[1];

            for (String w : words) {
                if (w.equals(term)) {
                	// write the mapper output
                    context.write(new Text(file_name), new Text(tfidf));
                }
            }
        }
    }

    //Reducer for Search
    public static class Reduce extends Reducer<Text, Text, Text, DoubleWritable> {
            
        @Override
        public void reduce(Text word, Iterable<Text> list, Context context)
                throws IOException, InterruptedException {
            double final_score = 0.0;
            //count the total TFIDF of each term
		for (Text a : list) {
			final_score = final_score + Double.parseDouble(a.toString());
            }
            // write the output to file
            context.write(new Text(word), new DoubleWritable(final_score));

        }
    }
}