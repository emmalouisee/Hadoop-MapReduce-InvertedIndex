package invertedindex;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


/**
 * This class reduces a set of Text values which share the same key to a single Text value
 * @author elr17
 *
 */
public  class ArticleReducer extends
Reducer<Text, Text, Text, Text> {
	
	//create strings to store the total occurrences of the word, docId and lineID+posID pairs
	String docID;
	ArrayList<String> line_pos;
	int total = 0;
	//create a hash map to store all docID with a list of lineID and posID pairs
	HashMap<String, ArrayList<String>> occurrence; // Stores the docid, along with line and position

	//variables to temporarily store information throughout the reduce method
	ArrayList<String> temp;
	String[] info;
	Map.Entry<String, ArrayList<String>> pair;
	
	//create an iterable
	Iterator<Entry<String, ArrayList<String>>> it;
	
	/**
	 * This method is called once for every key and reduces the iterable of Text values (containg a document ID and a list of lineID + positionID pairs) down to a single Text value containing the word and the number of its occurrences;
	 * the document IDs each word appears in and the number of times; the associated line Id and position ID pairs
	 * 
	 * @param key The Text object that the values are grouped by
	 * @param values The iterable of Text objects that are associated with the given key
	 */
	public void reduce(Text key, Iterable<Text> values,
			Context context) throws IOException, InterruptedException {
		
		//increment the number of reduce calls counter
		context.getCounter("Custom Counters: ", "Reduce Calls: ").increment(1);
		
		int division = (int)Math.ceil(31.0/(double)context.getNumReduceTasks());
		int partition = key.getLength()/division;
		
		context.getCounter("Partition Sizes:", "Partition " + partition+" ").increment(1);
		
		//create a hash map to store all docID with a list of lineID and posID pairs
		occurrence = new HashMap<String, ArrayList<String>>(); // Stores the docid, along with line and position
		
			//for each value 
			for (Text val : values) {
				
				//split the value into docID and lineId+posID pairs
				info = val.toString().split("\t");
				
				//if the value has the correct number of elements...
				if(info.length == 2) {
					
					//extract the docID 
					docID = info[0];
					//split the second array element by commas into individual lineId+posID pairs and store the pairs in a list
					line_pos = new ArrayList<String>(Arrays.asList(info[1].split(",")));
					
					//if we have already seen this docID before..
					if(occurrence.containsKey(docID)) {
				
						//get the list of lineID+posID pairs already stored
						temp = occurrence.get(docID);
						//add the list of new pairs to the list
						temp.addAll(line_pos);
						//store the updated list in the hash map
						occurrence.replace(docID, temp);
					} else {
					
						//put the list of new lineID+posID pairs into a list
						temp = new ArrayList<String>();
						temp.addAll(line_pos);
						//store the updated list in the hash map
						occurrence.put(docID, temp);
					}
				}
			}
			
			//create an iterable to iterate over each item in the hash map
			it = occurrence.entrySet().iterator();
	
			//while there are more items in the hash map...
			while (it.hasNext()) {
				//extract the item
				pair = (Map.Entry<String, ArrayList<String>>)it.next();
				//store the list of lineID+posID pairs
				line_pos = pair.getValue();
				//increase the total by the number of lineID+posID pairs
				total += line_pos.size();
			}
			
			//output the word and its total occurrences
			context.write(new Text(key + "  " + total), new Text(""));
			
			//increment Total Number of Words Counter
			context.getCounter("Custom Counters: ", "Number of Words: ").increment(total);
			//increment Number of Unique Words Counter
			context.getCounter("Custom Counters: ", "Number of Unique Words: ").increment(1);
			
			//Initialize the iterable to iterate over each item in the hash map again
			it = occurrence.entrySet().iterator();
				
			//clear the docId and lineID+posID variables holders
			docID = "";
			line_pos = new ArrayList<String>();
	
			//while there are more items in the hash map...
			while (it.hasNext()) {
				
				//extract the item
				pair = (Map.Entry<String, ArrayList<String>>)it.next();
			        
				//store the docID of the item
		        docID = pair.getKey();
		        //store the list of lineID+posID pairs
		        line_pos = pair.getValue();
		        //order the list in ascending order of lineID
		        line_pos.sort(null);
			        
		        //output the docID and the number of occurrences of the word in that document (indented by a single tab character)
			    context.write(new Text("\t" + docID + "  " + line_pos.size()), null);
			    
			    //for each lineID+posID pair in the list...
			    for(String lp : line_pos) {
			    	//output each pair (indented by two tab characters)
			    	context.write(new Text("\t\t" + lp), null);
			    } 
			}
	
			//output an empty line
			context.write(new Text(" "), null);		
	}
}