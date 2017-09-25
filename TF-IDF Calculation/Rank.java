package org.myorg;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

//Create a class with the name Rank
public class Rank {
	
 
	public static void main(String [] args) {

		 // The name of the file to open.

		String fileName = args[0]+"/part-r-00000";
		String dest = args[1];

		
		

        // This will reference one line at a time
        String line = null;
        String file_name = "", tfidf = "";
        ArrayList<Output> al = new ArrayList<Output>();
        double score = 0.0;

        try {
            // FileReader reads text files in the default encoding.
            FileReader fileReader = new FileReader(fileName);

            // Always wrap FileReader in BufferedReader.
            BufferedReader bufferedReader = new BufferedReader(fileReader);

            while((line = bufferedReader.readLine()) != null) {
                //System.out.println(line);
                // Splits the line using tab
                file_name = line.split("\t")[0];
                tfidf = line.split("\t")[1];
                // convert the TFIDF to Double and store in a ArraList
                score = Double.parseDouble(tfidf);
                Output op = new Output();
                op.setFileName(file_name);
                op.settFIDF(score);
                al.add(op);
            }
            

            // Always close files.
            bufferedReader.close();  
            //Sort the ArrayList on TFIDF
            Collections.sort(al, new Comparator<Output>() {

				@Override
				public int compare(Output o1, Output o2) {
					// TODO Auto-generated method stub
					return Double.compare(o2.gettFIDF(), o1.gettFIDF());
				}
            	
            });

            // Call a method to write the data to output
            writeData(dest, al);
            
        }
        catch(FileNotFoundException ex) {
            System.out.println(
                "Unable to open file '" + 
                fileName + "'");                
        }
        catch(IOException ex) {
            System.out.println(
                "Error reading file '" 
                + fileName + "'");                  
            // Or we could just do this: 
            // ex.printStackTrace();
        }
        
      
    }
	
	// method to write data to the output file
	private static void writeData(String dest, ArrayList<Output> list) {
		
		// create the directory with the path given in args[1]
		File file = new File(dest);
		String outputFileName = dest+"/part-r-00000.txt";
		boolean success = file.mkdir();	
		if (success){
			try {
	            // Assume default encoding.
	            FileWriter fileWriter = new FileWriter(outputFileName);

	            // Always wrap FileWriter in BufferedWriter.
	            BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);

	            // Note that write() does not automatically
	            // append a newline character.
	            for (Output o : list){
	            	String line = o.getFileName()+"\t"+o.gettFIDF();
	            	bufferedWriter.write(line);
	            	bufferedWriter.newLine();
	            }

	            // Always close files.
	            bufferedWriter.close();
	        }
	        catch(IOException ex) {
	            System.out.println(
	                "Error writing to file '"
	                + outputFileName + "'");
	            // Or we could just do this:
	            // ex.printStackTrace();
	        }
		}
		else{
			System.out.println("Output Directory creation failed");
			
		}
		

        
    }
		

	// class to store the file name and tfidf value
	static class Output{
		String fileName;
		Double tFIDF;
		public String getFileName() {
			return fileName;
		}
		public void setFileName(String fileName) {
			this.fileName = fileName;
		}
		public Double gettFIDF() {
			return tFIDF;
		}
		public void settFIDF(Double tFIDF) {
			this.tFIDF = tFIDF;
		}
		@Override
		public String toString() {
			return "Output [fileName=" + fileName + ", tFIDF=" + tFIDF + "]";
		}
		
		
	}
}
