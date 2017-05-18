package com.ldsa.mapreduce;

import java.io.IOException;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LogMapper extends Mapper<Object, List<Text>, Text, Text> {

	private Text year = new Text();
	private final static Text one = new Text("1");

	public void map(Object key, List<Text> values, Context context) throws InterruptedException, IOException {
		System.out.println("Mapper started" +values);
		year.set(getYear(values.get(2).toString()));
		context.write(year, one);
		System.out.println("Mapper Completed");
	}

	private String getYear(String string) {
		
		String year = "1900";
		try {
			Calendar calendar = new GregorianCalendar();
			calendar.setTime(new Date(string));
			year = String.valueOf( calendar.get(Calendar.YEAR));
		} catch (Exception e) {
			e.printStackTrace();
		}
		return year;
		
	}


}
