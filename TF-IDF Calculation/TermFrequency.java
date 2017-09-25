/*
 Name: Harish Pendyala
 Student ID# 800956847
 */
package org.myorg;

import java.io.File;
import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;


//Create a class with name TermFrequency
public class TermFrequency extends Configured implements Tool {

   private static final Logger LOG = Logger .getLogger( TermFrequency.class);

   public static void main( String[] args) throws  Exception {
      int res  = ToolRunner .run( new TermFrequency(), args);
      System .exit(res);
   }

   public int run( String[] args) throws  Exception {
	   // create a job to execute TermFrequecny
      Job job  = Job .getInstance(getConf(), " termfrequency ");
      job.setJarByClass( this .getClass());
      //Get the input from the destination provided in args[0] and write the output to the destination provided in args[1]
      FileInputFormat.addInputPaths(job,  args[0]);
      FileOutputFormat.setOutputPath(job,  new Path(args[ 1]));
      // Assign the Map and Reduce classes
      job.setMapperClass( Map .class);
      job.setReducerClass( Reduce .class);
      // set input and output type
      job.setOutputKeyClass( Text .class);
      job.setOutputValueClass( IntWritable .class);

      return job.waitForCompletion( true)  ? 0 : 1;
   }
   
   // Mapper for TermFrequency 
   public static class Map extends Mapper<LongWritable ,  Text ,  Text ,  IntWritable > {
      private final static IntWritable one  = new IntWritable( 1);
      private Text word  = new Text();


      private static final Pattern WORD_BOUNDARY = Pattern .compile("\\s*\\b\\s*");
      
     
      public void map( LongWritable offset,  Text lineText,  Context context)
        throws  IOException,  InterruptedException {

         String line  = lineText.toString();

         // convert the line to Lowercase
         line = line.toLowerCase();
         Text currentWord  = new Text();
         //split the line based on the RegEx Pattern given, and create the output of Map phase


         for ( String word  : WORD_BOUNDARY .split(line)) {
            if (word.isEmpty()) {
               continue;
            }
            //Retrieve the file name
            FileSplit fileSplit = (FileSplit)context.getInputSplit();
            String filename = fileSplit.getPath().getName();
            //insert the ##### delimiter between word and filename
            word = word+"#####"+filename;
            currentWord  = new Text(word);
            // write the output 
            context.write(currentWord,one);
         }
      }
   }

   // Reducer for DocWordCount
   public static class Reduce extends Reducer<Text ,  IntWritable ,  Text ,  DoubleWritable > {
      @Override 
      public void reduce( Text word,  Iterable<IntWritable > counts,  Context context)
         throws IOException,  InterruptedException {
         int sum  = 0;
       //count the occurrences of each word and write to the output file after calculating the TermFrequency
         for ( IntWritable count  : counts) {
            sum  += count.get();
         }
         // calculate the TF using 1+log10(number of occurrences of term in a file)
         double value=0.0;
         if (sum > 0){
        	 value =  (1+(Math.log10(sum)));

         }

         context.write(word,  new DoubleWritable(value));
      }
   }
}
