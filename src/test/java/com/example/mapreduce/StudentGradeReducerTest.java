/*
 * (c) Cloud for Beginners.
 * 
 * author: tmusabbir
 */
package com.example.mapreduce;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mrunit.MapReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;


/**
 * The Class StudentGradeReducerTest.
 */
public class StudentGradeReducerTest {

	/** The test mapper. */
	private Mapper<LongWritable, Text, Text, IntWritable> testMapper;

	/** The test reducer. */
	private Reducer<Text, IntWritable, Text, Text> testReducer;

	/** The test driver. */
	private MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, Text> testDriver;


	/**
	 * Setup.
	 */
	@Before
	public void setup() {
		testMapper = new StudentGradeMapper();
		testReducer = new StudentGradeReducer();
		testDriver = MapReduceDriver.newMapReduceDriver(testMapper, testReducer);
	}


	/**
	 * Test grade point.
	 *
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	@Test
	public void testGradePoint() throws IOException {
		String input = StudentGradeMapperTest.getDummyInput();
		Pair<LongWritable, Text> pair = new Pair<LongWritable, Text>(new LongWritable(1), new Text(input));
		testDriver.addCacheFile("D:\\workspaces-practice\\student-grade-dist-cache\\data\\input\\grade.txt");
		List<Pair<Text, Text>> results = testDriver.withInput(pair).run();

		for (Pair<Text, Text> p : results) {
			if (p.getFirst().toString().equalsIgnoreCase("Robin")) {
				System.out.println(p.getFirst() + " === " + p.getSecond());
				assertEquals(p.getSecond().toString(), "A");
			}
		}
	}
}
