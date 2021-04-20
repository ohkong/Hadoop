package test0125;

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
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class MatroMultipleOutPutEx {
	public static void main(String[] args) throws Exception {
		String in = "infile/metro.csv";
		String out = "outfile/multi-matro";
		Configuration conf = new Configuration(); 
	    // Job 이름 설정
	    Job job = new Job(conf, "MatroMultipleOutPutEx");
	    // 입출력 데이터 경로 설정
	    FileInputFormat.addInputPath(job, new Path(in));
	    FileOutputFormat.setOutputPath(job, new Path(out));
	    //출력 파일 존재시 출력파일 제거
	    FileSystem hdfs = FileSystem.get(conf);
		if (hdfs.exists(new Path(out))) { 
			hdfs.delete(new Path(out), true);
			System.out.println("기존 출력파일 삭제");
		}      
	    // Job 클래스 설정
	    job.setJarByClass(MatroMultipleOutPutEx.class);
	    // Mapper 클래스 설정
	    job.setMapperClass(MatroMapperWithMultipleOutputs.class);
	    // Reducer 클래스 설정
	    job.setReducerClass(MatroReducerWithMultipleOutputs.class);
	    // 입출력 데이터 포맷 설정
	    job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
	    // 출력키 및 출력값 유형 설정
	    job.setOutputKeyClass(Text.class); 
	    job.setOutputValueClass(IntWritable.class);

	    // MultipleOutputs 설정
	    MultipleOutputs.addNamedOutput
	              (job, "enter", TextOutputFormat.class, Text.class, IntWritable.class);
	    MultipleOutputs.addNamedOutput
	              (job, "exit", TextOutputFormat.class,Text.class, IntWritable.class);
	    job.waitForCompletion(true);
	}
}