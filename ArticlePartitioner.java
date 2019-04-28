package invertedindex;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;


/**
 * This class partitions the key space, using the length of the key
 * @author elr17
 *
 */
public class ArticlePartitioner extends Partitioner<Text, Text> {
	
	/**
	 * This method partitions the keys based on their length and the number of partitions available
	 * 
	 * @param key The key to be partitioned
	 * @param value The entry value
	 * @param numPartitions The total number of partitions(based on the number of reducers)
	 * @return An integer specifying which partition the key must be placed in
	 */
	@Override
	public int getPartition(Text key, Text value, int numPartitions) {
		
		//31.0 = the length of the longest word on core100 + 1 so words are partitioned correctly
		return key.getLength()/((int)Math.ceil(31.0/(double)numPartitions));
	}

}