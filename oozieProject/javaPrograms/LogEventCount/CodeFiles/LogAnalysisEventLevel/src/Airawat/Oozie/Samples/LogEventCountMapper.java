package Airawat.Oozie.Samples;

import java.io.IOException;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;


public class LogEventCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {


	
	String strLogEntryPattern = "(\\w+)\\s+(\\d+)\\s+(\\d+:\\d+:\\d+)\\s+(\\w+\\W*\\w*)\\s+(.*?\\:)\\s+(.*$)";
	public static final int NUM_FIELDS = 6;
	Text strEvent = new Text("");
	

	
  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {

	String strLogEntryLine = value.toString();
    Pattern objPtrn = Pattern.compile(strLogEntryPattern);
	
	Matcher objPatternMatcher = objPtrn.matcher(strLogEntryLine);
	if (!objPatternMatcher.matches() || NUM_FIELDS != objPatternMatcher.groupCount()) {
		System.err.println("Bad log entry (or problem with RE?):");
		System.err.println(strLogEntryLine);
		return;
	}
	/*
	System.out.println("Month_Name: " + objPatternMatcher.group(1));
	System.out.println("Day: " + objPatternMatcher.group(2));
	System.out.println("Time: " + objPatternMatcher.group(3));
	System.out.println("Node: " + objPatternMatcher.group(4));
	System.out.println("Process: " + objPatternMatcher.group(5));
	System.out.println("LogMessage: " + objPatternMatcher.group(6));
	*/
	 
	strEvent.set(((FileSplit)context.getInputSplit()).getPath().toString().substring((((FileSplit)context.getInputSplit()).getPath().toString().length()-16), (((FileSplit)context.getInputSplit()).getPath().toString().length()-12)) + "-" + ((objPatternMatcher.group(5).toString().indexOf("[")) == -1 ? (objPatternMatcher.group(5).toString().substring(0,(objPatternMatcher.group(5).length()-1))) : (objPatternMatcher.group(5).toString().substring(0,(objPatternMatcher.group(5).toString().indexOf("["))))));

    context.write(strEvent, new IntWritable(1));
      
  }
}
