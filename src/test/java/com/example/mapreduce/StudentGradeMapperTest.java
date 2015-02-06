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
import org.apache.hadoop.mrunit.MapDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;


/**
 * The Class StudentGradeMapperTest.
 */
public class StudentGradeMapperTest {

	/** The test mapper. */
	private Mapper<LongWritable, Text, Text, IntWritable> testMapper;

	/** The test driver. */
	private MapDriver<LongWritable, Text, Text, IntWritable> testDriver;


	/**
	 * Setup.
	 */
	@Before
	public void setup() {
		testMapper = new StudentGradeMapper();
		testDriver = MapDriver.newMapDriver(testMapper);
	}


	/**
	 * Test grade.
	 * 
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	@Test
	public void testGrade() throws IOException {
		String input = StudentGradeMapperTest.getDummyInput();

		List<Pair<Text, IntWritable>> results = testDriver.withInput(new LongWritable(1), new Text(input)).run();
		boolean found = false;
		for (Pair<Text, IntWritable> p : results) {
			if (p.getFirst().toString().equalsIgnoreCase("John") && p.getSecond().get() == 40) {
				found = true;
				System.out.println(p.getFirst() + " === " + p.getSecond());
			}
		}
		assertEquals(found, true);
	}


	/**
	 * Gets the dummy input.
	 * 
	 * @return the dummy input
	 */
	public static String getDummyInput() {
		StringBuffer input = new StringBuffer();
		input.append("John	40");
		input.append(StudentGradeDistCache.NEW_LINE);
		input.append("Robin	25");
		input.append(StudentGradeDistCache.NEW_LINE);
		input.append("John	45");
		input.append(StudentGradeDistCache.NEW_LINE);
		input.append("Peter	15");
		input.append(StudentGradeDistCache.NEW_LINE);
		input.append("Robin	50");
		input.append(StudentGradeDistCache.NEW_LINE);
		input.append("Robin	20");
		input.append(StudentGradeDistCache.NEW_LINE);
		input.append("Peter	50");
		input.append(StudentGradeDistCache.NEW_LINE);
		input.append("Don	40");
		return input.toString();
	}
}
