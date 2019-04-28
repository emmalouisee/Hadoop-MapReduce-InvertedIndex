package invertedindex;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;


/**
 * This class maps input KEY/VALUE pairs (VALUE = document with a document header) to a set of 
 * intermediate KEY/VALUE pairs (KEY=individual word, VALUE=documentID lineID positonID)
 * 
 * @author elr17
 *
 */
public class ArticleMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	//create Text objects to store the output and the word
	private Text outputValue = new Text();
	private Text word = new Text();
	
	//create a list to store all the stop words
	private ArrayList<String> stopWords;
	
	//create variables to store the document and its ID, and to store a line and its ID, and the word ID
	String doc;
	String docID;
	String line;
	int lineID;
	int wordID;
	
	//create a tokenizer to store the lines
	StringTokenizer tokenLine;
	//create a tokenizer to store the words
	StringTokenizer tokenWord;
	
	
	
	/**
	 * This method creates a list of stop words taken from a file containing the stop words
	 */
	@Override
    public void setup(Context context) throws IOException,
        InterruptedException {
	      Configuration conf = context.getConfiguration();
	
	        URI[] patternsURIs = Job.getInstance(conf).getCacheFiles();
	        for (URI patternsURI : patternsURIs) {
	        Path stopWordPath = Paths.get(patternsURI.getPath());
	        String content = new String(Files.readAllBytes(stopWordPath));
	        stopWords = new ArrayList<String>(Arrays.asList(content.split("\n")));
        }
    }

	/**
	 * This method is called once for each KEY/VALUE (VALUE = document with a document header) pair and 
	 * writes a new KEY/VALUE pair (KEY=individual word, VALUE=documentID lineID positonID) to the context given
	 */
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		//increment the Number of Map Calls Counter
		context.getCounter("Custom Counters: ", "Number of Map Calls: ").increment(1);
		
		//store the document passed as the value
		doc = value.toString();
		//split the document into lines
		tokenLine = new StringTokenizer(doc, "\n");
	
		//while there are still lines left..
		while(tokenLine.hasMoreTokens()) {
			
			//get a line
			docID = tokenLine.nextToken();
			//extract the document ID
		    docID = docID.substring(14);
		    docID = docID.substring(0,docID.length()-2);
		

			lineID = -1;
			
			//while there are still more lines..
			while (tokenLine.hasMoreTokens()) {
				
				//increment the lineID
				lineID++;	
				//get a line
				line = tokenLine.nextToken();
				//split the line by spaces space
				tokenWord = new StringTokenizer(line);
				
				//create a variable to store the wordID
				wordID = -1;
				
				//while there are still words left..
				while(tokenWord.hasMoreTokens()) {
					
					//increment the wordID
					wordID++;
					//get a word
					word.set(tokenWord.nextToken());
					
					//if the word is NOT a stop word and doesnt contain any non-word chars or numbers...
					if(!stopWords.contains(word.toString())) {						
						if(!(word.toString()).matches(".*\\W+.*") && !(word.toString()).matches(".*[1-9]+.*")) {
							
							//store in the outputValue Text object the docID, lineID and posID separated by a tab character
							outputValue.set(docID + "\t" + lineID + "\t" + wordID);
							//output the outputValue string to the context given
							context.write(word, outputValue);
						}
					} else {
						//increment Number of StopWords Counter
						context.getCounter("Custom Counters: ", "Number of Stop-Words: ").increment(1);
					}
				}
			}
		}
	}
}
