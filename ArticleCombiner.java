package invertedindex;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


/**
 * This class reduces a set of Text values produced from ArticleMapper which share the same key to a smaller set of Text values
 * @author elr17
 *
 */
public  class ArticleCombiner extends
Reducer<Text, Text, Text, Text> {
	
	//create strings to store the total occurrences of the word, docId and lineID+posID pairs
	String docID;
	String lineID_posID;
	ArrayList<String> line_pos;
	int total = 0;
	
	//create strings to store the output
	String result;
			
	//create a hash map to store all docID with a list of lineID and posID pairs
	HashMap<String, ArrayList<String>> occurrence; // Stores the docid, along with line and position
	
	//variables to temporarily store information throughout the reduce method
	ArrayList<String> temp;
	String[] info;
	Map.Entry<String, ArrayList<String>> pair;
	
	
	
	
	
	/**
	 * This method is called once for every key and reduces the iterable of Text values down to a single Text value containing document IDs, 
	 * and their associated line Id and position ID pairs 
	 * 
	 * @param key The Text object that the values are grouped by
	 * @param values The iterable of Text objects that are associated with the given key
	 */
	public void reduce(Text key, Iterable<Text> values,
			Context context) throws IOException, InterruptedException {
		
		//increment the Number of Combiner Calls Counter
		context.getCounter("Custom Counters: ", "Number of Combine Calls: ").increment(1);
		
		//create a hash map to store all docID with a list of lineID and posID pairs
		occurrence = new HashMap<String, ArrayList<String>>(); // Stores the docid, along with line and position
		
		//for each value 
		for (Text val : values) {
			
			//split the value into docID and lineId+posID pair
			info = val.toString().split("\t");
			
			//if the value has the correct number of elements...
			if(info.length == 3) {
				//extract the docID 
				docID = info[0];
				//extract the lineID and posID pair
				lineID_posID = info[1] + " " + info[2];
			
				//if we have already seen this docID before..
				if(occurrence.containsKey(docID)) {
			
					//get the list of lineID+posID pairs already stored
					temp = occurrence.get(docID);
					//add the new pair to the list
					temp.add(lineID_posID);
					//store the updated list in the hash map
					occurrence.put(docID, temp);
				} else {
					
					//put the lineID+posID pair into a list
					temp = new ArrayList<String>();
					temp.add(lineID_posID);
					//store the updated list in the hash map
					occurrence.put(docID, temp);
				}
			}
		}
		
		//create an iterable to iterate over each item in the hash map
		Iterator<Entry<String, ArrayList<String>>> it = occurrence.entrySet().iterator();			
			
		//while there are more items in the hash map...
		while (it.hasNext()) {
			//extract the item
		    pair = (Map.Entry<String, ArrayList<String>>)it.next();
		    //store the docID of the item
		    docID = pair.getKey();
		    //store the list of lineID+posID pairs
		    line_pos = pair.getValue();
		        
		    //store the docID in the result String followed by a tab character
		    result = docID + "\t";
		        
		    //for each lineID+posID pair in the list
		    for(String lp : line_pos) {
		     	//store the pair in the result string separated by a comma
		       	result = result + lp + ",";	      	        	
		    } 
		    //remove the trailing comma from the result string
		    result = result.substring(0, result.length()-1);
		    //output the result string
		    context.write(key, new Text(result));
		}
	}
}
