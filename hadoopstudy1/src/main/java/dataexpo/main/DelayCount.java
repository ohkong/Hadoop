package dataexpo.main;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import dataexpo.mapreduce.DelayCountMapperWithCounter;
import dataexpo.mapreduce.DelayCountReducer;
import dataexpo.mapreduce.DelayCounters;

//옵션값에 따라서 출발지연정보, 도착지연정보를 하나의 프로그램에서 처리하기
//정시출발, 지연출발, 일찍출발 등의 정보들을 출력하기
public class DelayCount extends Configured implements Tool{
	public static void main(String[] args) throws Exception {
	//	String arg[] = {"-D","workType=departure","infile/1987.csv","outfile/d-1987"};
		String arg[] = {"-D","workType=arrival","infile/1987.csv","outfile/a-1987"};
		int res = ToolRunner.run(new Configuration(),new DelayCount(), arg);
		System.out.println("MR-Job Result:" + res);
	}
	
	public int run(String[] args) throws Exception{
		//otherArgs : args배열 중 앞2개 요소를 제외한 나머지. 입력파일, 출력파일 정보
		String[] otherArgs = new GenericOptionsParser(getConf(),args).getRemainingArgs();
		if(otherArgs.length != 2) {
			System.err.println("Usage: DelayCount <in> <out>");
			System.exit(2);
		}
		System.out.println(otherArgs[0] + "," + otherArgs[1]);
		Job job = new Job(getConf(), "DelayCount");
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		job.setJarByClass(DelayCount.class);
		job.setMapperClass(DelayCountMapperWithCounter.class);
		job.setReducerClass(DelayCountReducer.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		//출력 파일 존재시 출력파일 제거
		FileSystem hdfs = FileSystem.get(getConf());
		if(hdfs.exists(new Path(otherArgs[1]))) {
			hdfs.delete(new Path(otherArgs[1]),true);
			System.out.println("기존 출력파일 삭제");
		}
		
		job.waitForCompletion(true);//작업 실행
		for(DelayCounters d : DelayCounters.values()) {
			long tot = job.getCounters().findCounter(d).getValue();
			System.out.println(d + ":" + tot);
		}
		return 0;
	}

}
