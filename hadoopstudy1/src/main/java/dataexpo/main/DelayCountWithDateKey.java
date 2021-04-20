package dataexpo.main;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import dataexpo.DateKey;
import dataexpo.mapreduce.DelayCountMapperWithDateKey;
import dataexpo.mapreduce.DelayCountReducerWithDateKey;



public class DelayCountWithDateKey {
	public static void main(String[] args) throws Exception{
		String in = "infile/1988.csv";
		String out = "outfile/multi-1988";
		Configuration conf = new Configuration();
		Job job = new Job(conf,"DelayCountWithDateKey");
		FileInputFormat.addInputPath(job,new Path(in));
		FileOutputFormat.setOutputPath(job, new Path(out));
		
		FileSystem hdfs = FileSystem.get(conf);
		if (hdfs.exists(new Path(out))) {
			hdfs.delete(new Path(out), true);
			System.out.println("기존 출력파일 삭제");
		}
		
		job.setJarByClass(DelayCountWithDateKey.class);
		job.setMapperClass(DelayCountMapperWithDateKey.class);
		job.setReducerClass(DelayCountReducerWithDateKey.class);
		job.setMapOutputKeyClass(DateKey.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(DateKey.class);
		job.setOutputValueClass(IntWritable.class);
		
		//MultipleOutputs 설정
		MultipleOutputs.addNamedOutput(job, "departure",
				TextOutputFormat.class, DateKey.class, IntWritable.class);
		MultipleOutputs.addNamedOutput(job, "arrival",
				TextOutputFormat.class, DateKey.class, IntWritable.class);
		
		job.waitForCompletion(true);
	}

}
