package invertedindex;

import org.apache.hadoop.fs.Path;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;

/**
 * This program will will produce the full inverted index of each word in the input files, excluding stop words
 * 
 * @author elr17
 *
 */
public class InvertedIndex extends Configured implements Tool {

	
	public int run(String[] args) throws Exception {
	        Configuration conf = new Configuration();
	        conf.set("textinputformat.record.delimiter", "</Document>");

			Job job = Job.getInstance(conf,"inverted index");
			job.setJarByClass(InvertedIndex.class);
		 
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
	
			job.setMapperClass(ArticleMapper.class);
			job.setReducerClass(ArticleReducer.class);
			job.setNumReduceTasks(2);
			
			job.setCombinerClass(ArticleCombiner.class);
			job.setSortComparatorClass(ArticleComparator.class);
			job.setPartitionerClass(ArticlePartitioner.class);
			
			
			job.addCacheFile(new Path("stop-words.txt").toUri());
			
			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);
	
			FileInputFormat.setInputPaths(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));			
			
			boolean success = job.waitForCompletion(true);

			return success ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		final int result;
		if (args.length < 2) {
			System.out.println("Arguments: inputDirectory outputDirectory");
			result = -1;
		} else {
			result = ToolRunner.run(new InvertedIndex(), args);
		}
		System.exit(result);
	}
}