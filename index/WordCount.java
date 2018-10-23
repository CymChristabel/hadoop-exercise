import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.commons.io.FilenameUtils;

import org.apache.hadoop.conf.Configuration;

public class WordCount {
	
	public static class WordCountMapper extends Mapper<LongWritable, Text, Text, Text>{
		private Text word = new Text();
		
		public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException
		{
			// Reading file name			
			FileSplit fileSplit = (FileSplit) context.getInputSplit();
			String fileName = fileSplit.getPath().getName();
			fileName = FilenameUtils.removeExtension(fileName);
			

			// Reading input one line at a time and tokenizering.
			String line = value.toString().toLowerCase();
			String[] lineSplittedByPunch = line.split("[^a-z]+");
			
			for(String w : lineSplittedByPunch)
			{
				context.write(new Text(w), new Text(fileName));
			}
		}
	}
	
	public static class WordCountReducer extends Reducer<Text, Text, Text, Text>
	{
		// Reduce method collects the output of the Mapper and adds the 1's to get the word's count.
		public void reduce(Text key, Iterable<Text> values, Context context) 
			throws IOException, InterruptedException
		{
			HashMap<String, Integer> hashMap = new HashMap<String, Integer>();
			for(Text value : values)
			{
				String temp = value.toString();
				if(hashMap.get(temp) != null)
				{
					hashMap.put(temp, hashMap.get(temp) + 1);
				}
				else
				{
					hashMap.put(temp, 1);
				}
			}
			String res = "";
			for(Map.Entry<String, Integer> entry : hashMap.entrySet())
			{
				res = res + entry.getKey() + ":" + entry.getValue().toString() + " ";
			}
			context.write(key, new Text(res));
		}
	}

	
	public static void main(String[] args) 
			throws IOException, ClassNotFoundException, InterruptedException
	{
		if(args.length != 2)
		{
			System.err.println("Usage: Word Count <input path> <output_path>");
			System.exit(-1);
		}
		
		Job job = new Job(new Configuration());
		job.setJarByClass(WordCount.class);
		job.setJobName("Word Count");
		
		// HDFS I/O directories to be fetched from the Dataproc job submission console
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		// Providing the mapper and reducer class names.
		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReducer.class);
		
		// Setting the job object with the data types of output key(text) and value(IntWritable)
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
