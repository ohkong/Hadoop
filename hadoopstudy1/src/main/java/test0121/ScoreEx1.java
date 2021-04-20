package test0121;

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
public class ScoreEx1 {
	public static void main(String[] args) 
   	 throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration(); //하둡의 환경설정.
		String in = "infile/score.txt";
		String out = "outfile/score";  //하둡서버에 저장되는 위치.블럭화하여 저장.
		Job job = new Job(conf,"ScoreEx1"); //작업 객체
		job.setJarByClass(ScoreEx1.class);  //작업 객체의 자료형 설정.
		//맵리듀스 : 맵퍼 + 리듀서 
		job.setMapperClass(ScoreMapper.class);  //맵퍼 클래스 설정
		job.setReducerClass(ScoretReducer.class); //리듀서 클래스 설정
		job.setInputFormatClass(TextInputFormat.class); //입력데이터 자료형 설정
		job.setOutputFormatClass(TextOutputFormat.class); //출력 데이터 자료형 설정
		job.setMapOutputKeyClass(Text.class);           //키데이터 자료형 설정
		job.setMapOutputValueClass(IntWritable.class);  //value 데이터 자료형 설정.
		FileInputFormat.addInputPath(job, new Path(in)); //입력데이터의 저장파일 설정.
		FileOutputFormat.setOutputPath(job, new Path(out)); //출력데이터 저장파일의 위치 설정
	    FileSystem hdfs = FileSystem.get(conf);
	    if(hdfs.exists(new Path(out))) { 
	    	hdfs.delete(new Path(out),true); //출력파일의 경로 존재하는 경우 삭제.
	    	System.out.println("기존 출력파일 삭제");
	    }		
		
		job.waitForCompletion(true); //작업 시작.
	}
}