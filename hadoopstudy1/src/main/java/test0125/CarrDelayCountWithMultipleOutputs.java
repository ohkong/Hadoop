package test0125;

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

import dataexpo.mapreduce.DelayCountMapperWithMultipleOutputs;
import dataexpo.mapreduce.DelayCountReducerWithMultipleOutputs;
/*
 * 하나의 출력결과 폴더에 출발지연 정보와, 도착지연 정보 파일을 생성하기
 */
public class CarrDelayCountWithMultipleOutputs {
  public static void main(String[] args) throws Exception {
	String in = "infile/1988.csv";
	String out = "outfile/carr-multi-1988";
	Configuration conf = new Configuration(); 
    // Job 이름 설정
    Job job = new Job(conf, "CarrDelayCountWithMultipleOutputs");
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
    job.setJarByClass(CarrDelayCountWithMultipleOutputs.class);
    // Mapper 클래스 설정
    job.setMapperClass(CarrDelayCountMapperWithMultipleOutputs.class);
    // Reducer 클래스 설정
    job.setReducerClass(CarrDelayCountReducerWithMultipleOutputs.class);
    // 입출력 데이터 포맷 설정
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    // 출력키 및 출력값 유형 설정
    job.setOutputKeyClass(Text.class); //문자열 기반. "1988,1" 형태 => 1988,10 < 1988,2 
    job.setOutputValueClass(IntWritable.class);

    // MultipleOutputs 설정
    MultipleOutputs.addNamedOutput
              (job, "departure", TextOutputFormat.class, Text.class, IntWritable.class);
    MultipleOutputs.addNamedOutput
              (job, "arrival", TextOutputFormat.class,Text.class, IntWritable.class);
    job.waitForCompletion(true);
  }
}
