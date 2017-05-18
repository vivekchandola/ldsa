package com.ldsa.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class LogReducer extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		int tf = 0;
		
		for (Text a : values) {
			tf += Integer.parseInt(a.toString());
		}
		context.write(new Text(key.toString()), new Text(String.valueOf(tf)));

	}
}
