/*
 * (c) Cloud for Beginners.
 * 
 * author: tmusabbir
 */
package com.example.mapreduce;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The Class StudentGradeReducer.
 */
public class StudentGradeReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, Text> {

	/** The cache path. */
	private Path cachePath;

	/** The grade lists. */
	private final List<GradePoint> gradeLists = new ArrayList<GradePoint>();

	/** The Constant LOGGER. */
	private static final Logger LOGGER = LoggerFactory.getLogger(StudentGradeReducer.class);

	/**
	 * The Class GradePoint.
	 */
	public class GradePoint {

		/** The grade. */
		private final String grade;

		/** The min point. */
		private final int minPoint;


		/**
		 * Instantiates a new grade point.
		 * 
		 * @param g the g
		 * @param p the p
		 */
		public GradePoint(String g, int p) {
			this.grade = g;
			this.minPoint = p;
		}
	}


	/* (non-Javadoc)
	 * @see org.apache.hadoop.mapred.MapReduceBase#configure(org.apache.hadoop.mapred.JobConf)
	 */
	@Override
	public void configure(JobConf job) {
		try {
			Path[] localCacheFiles = DistributedCache.getLocalCacheFiles(job);
			if (localCacheFiles != null) {
				cachePath = localCacheFiles[0];
				recordGrade();
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}


	/**
	 * Record grade.
	 */
	private void recordGrade() {
		try {
			BufferedReader br = new BufferedReader(new FileReader(cachePath.toString()));
			String line = null;
			while ((line = br.readLine()) != null) {
				StringTokenizer tokenizer = new StringTokenizer(line);
				GradePoint gp = new GradePoint(tokenizer.nextToken(), Integer.parseInt(tokenizer.nextToken()));
				gradeLists.add(gp);
			}
		} catch (IOException ioe) {
			LOGGER.error("Failed to read grade from cache.", ioe);
		}
	}


	/* (non-Javadoc)
	 * @see org.apache.hadoop.mapred.Reducer#reduce(java.lang.Object, java.util.Iterator, org.apache.hadoop.mapred.OutputCollector, org.apache.hadoop.mapred.Reporter)
	 */
	public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, Text> output,
			Reporter reporter) throws IOException {
		int totalPoint = 0;
		while (values.hasNext()) {
			int p = values.next().get();
			totalPoint += p;
		}
		for (GradePoint gp : gradeLists) {
			if (totalPoint >= gp.minPoint) {
				output.collect(key, new Text(gp.grade));
				break;
			}
		}
	}

}
