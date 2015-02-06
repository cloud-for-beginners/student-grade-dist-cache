/*
 * (c) Cloud for Beginners.
 * 
 * author: tmusabbir
 */
package com.example.mapreduce;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;


/**
 * The Class StudentGradeMapper.
 */
public class StudentGradeMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {

	/* (non-Javadoc)
	 * @see org.apache.hadoop.mapred.Mapper#map(java.lang.Object, java.lang.Object, org.apache.hadoop.mapred.OutputCollector, org.apache.hadoop.mapred.Reporter)
	 */
	public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter)
			throws IOException {
		String[] lines = value.toString().split(StudentGradeDistCache.NEW_LINE);
		for (String line : lines) {
			StringTokenizer tokenizer = new StringTokenizer(line);
			Text name = new Text(tokenizer.nextToken());
			IntWritable point = new IntWritable(Integer.parseInt(tokenizer.nextToken()));
			output.collect(name, point);
		}
	}
}
