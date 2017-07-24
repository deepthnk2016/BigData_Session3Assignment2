package com.acadgild;

/**
 * Mapper class to filter out records having NA in company name or product name.
 */
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;

public class InvalidRecordsMapper extends Mapper<Object, Text, Text, Text> {
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

		// Split the input strings from the file on "|"
		String[] lineArray = value.toString().split("\\|");

		// Fetch the company name and product name
		String companyName = lineArray[0];
		String prodName = lineArray[1];

		// Initialize a variable to hold the output.
		Text output = new Text();
		output.set(value.toString());

		// If the company name or product name contains NA, then don't print that line.
		if (companyName.equals("NA") == false && prodName.equals("NA") == false) {
			// write the output
			context.write(output, new Text(""));
		}
	}
}
