/*
 * (c) Cloud for Beginners.
 * 
 * author: tmusabbir
 */
package com.example.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Log4jConfigurer;


/**
 * The Class StudentGradeDistCache.
 */
public class StudentGradeDistCache extends Configured implements Tool {

	/** The Constant LOGGER. */
	private static final Logger LOGGER = LoggerFactory.getLogger(StudentGradeDistCache.class);

	/** The Constant NEW_LINE. */
	public static final String NEW_LINE = "\n";


	/* (non-Javadoc)
	 * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
	 */
	public int run(String[] args) throws Exception {
		int ret = 0;
		LOGGER.info("Starting student grade job...");

		Path inputPath = new Path(args[0]);
		Path outputPath = new Path(args[1]);
		Path cachePath = new Path(args[2]);

		Configuration hadoopConf = getConf();
		JobConf job = new JobConf(hadoopConf, StudentGradeDistCache.class);
		job.setJarByClass(getClass());
		job.setJobName("Student Grade");

		FileInputFormat.setInputPaths(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);

		job.setInputFormat(TextInputFormat.class);
		job.setOutputFormat(TextOutputFormat.class);

		job.setMapperClass(StudentGradeMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setReducerClass(StudentGradeReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		DistributedCache.addCacheFile(cachePath.toUri(), job);

		JobClient.runJob(job);
		return ret;
	}


	/**
	 * The main method.
	 * 
	 * @param args the arguments
	 * @throws Exception the exception
	 */
	public static void main(String[] args) throws Exception {
		Log4jConfigurer.initLogging("classpath:META-INF/log4j.properties");
		StudentGradeDistCache wc = new StudentGradeDistCache();
		if ((args == null) || (args != null && args.length != 3)) {
			LOGGER.error("Usage: student-grade-dist-cache-0.0.1.jar [generic options] <input> <output> <grade>");
			ToolRunner.printGenericCommandUsage(System.err);
		} else {
			int exitCode = ToolRunner.run(wc, args);
			System.exit(exitCode);
		}
	}
}
