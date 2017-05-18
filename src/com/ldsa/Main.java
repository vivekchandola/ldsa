package com.ldsa;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.ldsa.io.CSVLineRecordReader;
import com.ldsa.io.CSVNLineInputFormat;
import com.ldsa.io.CSVTextInputFormat;
import com.ldsa.mapreduce.LogMapper;
import com.ldsa.mapreduce.LogReducer;

public class Main extends Configured implements Tool{
	
	
	public Main() {
		this(null);
	}

	public Main(Configuration conf) {
		super(conf);
	}

	public int run(String[] args) throws Exception {

		getConf().set(CSVLineRecordReader.FORMAT_DELIMITER, "\"");
		getConf().set(CSVLineRecordReader.FORMAT_SEPARATOR, ",");
		getConf().setInt(CSVNLineInputFormat.LINES_PER_MAP, 40000);
		getConf().setBoolean(CSVLineRecordReader.IS_ZIPFILE, false);
		getConf().set(CSVLineRecordReader.CSV_SEPARATOR, ",");

		Job csvJob = Job.getInstance(getConf(), "LDSA-YEAR");
		csvJob.setJarByClass(Main.class);

		csvJob.setMapperClass(LogMapper.class);

		csvJob.setReducerClass(LogReducer.class);
		csvJob.setNumReduceTasks(1);
		
		csvJob.setOutputKeyClass(Text.class);
		
		csvJob.setOutputValueClass(Text.class);

		FileInputFormat.setInputPaths(csvJob, new Path(args[0]));
		csvJob.setInputFormatClass(CSVTextInputFormat.class);

		FileOutputFormat.setOutputPath(csvJob, new Path(args[1]));

		System.out.println("Process will begin");
		csvJob.waitForCompletion(true);
		System.out.println("Process ended");

		return 0;
	}
	
	
	public static void main(String[] args) throws Exception {

		int res = -1;
		try {
			System.out.println("Initializing Main for CSV reader");
			Main importer = new Main();
			// Let ToolRunner handle generic command-line options and run hadoop
			res = ToolRunner.run(new Configuration(), importer, args);
			System.out.println("ToolRunner finished running hadoop");

		} catch (Throwable e) {
			e.printStackTrace();
		} finally {
			System.out.println("Quitting with execution code " + res);
			System.exit(res);
		}
	}

}
