package dataexpo.main;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import dataexpo.mapreduce.ArrivalDealyCountMapper;
import dataexpo.mapreduce.DelayCountReducer;

//월별 도착 지연 건수 분석
public class ArrivalDelayCount {
	public static void main(String[] args) throws Exception {
		String in = "infile/1987.csv";
		String out = "outfile/1987out";
		Configuration conf = new Configuration();
		Job job = new Job(conf,"ArrivalDelayCount");
		FileInputFormat.addInputPath(job, new Path(in));
		FileOutputFormat.setOutputPath(job, new Path(out));
		job.setJarByClass(ArrivalDelayCount.class);
		job.setMapperClass(ArrivalDealyCountMapper.class);
		job.setReducerClass(DelayCountReducer.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileSystem hdfs = FileSystem.get(conf);
		if(hdfs.exists(new Path(out))) {
			hdfs.delete(new Path(out),true);
			System.out.println("기존 출력파일 삭제");
		}
		
		job.waitForCompletion(true);
	}

}
