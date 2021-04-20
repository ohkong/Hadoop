package test0122;

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

import dataexpo.mapreduce.DelayCountReducer;


public class MetroEx {
	public static void main(String[] args) throws Exception {
		String in = "infile/metro.csv";
		String out = "outfile/metro2";
		Configuration conf = new Configuration();
		
		//Job 이름 설정
		Job job = new Job(conf, "MetroEx");
		//입출력 데이터 경로설정
		FileInputFormat.addInputPath(job, new Path(in));
		FileOutputFormat.setOutputPath(job, new Path(out));
		
		job.setJarByClass(MetroEx.class);
		job.setMapperClass(MetroMapper.class);
		job.setReducerClass(DelayCountReducer.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		//출력 파일 존재시 출력파일 제거
		FileSystem hdfs = FileSystem.get(conf);
		if(hdfs.exists(new Path(out))) {
			hdfs.delete(new Path(out),true);
			System.out.println("기존 출력파일 삭제");
		}
		job.waitForCompletion(true);
	}

}
