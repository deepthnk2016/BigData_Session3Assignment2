package com.acadgild;

/**
 * Main class to set and invoke the map reduce job.
 */
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Session3Assignment2 {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "Assignment3");
		job.setJarByClass(Session3Assignment2.class);

		// Set map output key and value
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		// Set output key and value
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// Set the mapper class
		job.setMapperClass(InvalidRecordsMapper.class);

		// Set the input and output format class
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		// Set the input and output directories
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);

	}

}
