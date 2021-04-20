package test0121;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
public class ScoreEx2 {
	public static void main(String[] args) throws IOException {
		JobConf conf = new JobConf(ScoreEx2.class); //작업 + 환경설정
		conf.setJobName("scoreex2");
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);
		conf.setMapperClass(Map.class);
//		conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(Reduce.class);
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		FileInputFormat.setInputPaths(conf, new Path("infile/score.txt"));
		FileOutputFormat.setOutputPath(conf, new Path("outfile/score2"));
	    FileSystem hdfs = FileSystem.get(conf);
	    if(hdfs.exists(new Path("outfile/score2"))) { 
	    	hdfs.delete(new Path("outfile/score2"),true); 
	    	System.out.println("기존 출력파일 삭제");
	    }		
		JobClient.runJob(conf); //작업 시작
	}
	public static class Map extends MapReduceBase 
	               implements Mapper<LongWritable, Text, Text, IntWritable> {
		private Text word = new Text();
		@Override
		public void map(LongWritable key, Text value, 
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			String[] lines = value.toString().split(",");
			word.set(lines[0]);
			output.collect(word, new IntWritable(Integer.parseInt(lines[1]))); 											
		}
	} // Map 내부 클래스 종료
	public static class Reduce extends MapReduceBase 
	               implements Reducer<Text, IntWritable, Text, IntWritable> {
		@Override
		public void reduce(Text key, Iterator<IntWritable> value, 
				OutputCollector<Text, IntWritable> output,
				Reporter reporter) throws IOException {
			int sum = 0;
			while (value.hasNext()) {
				sum += value.next().get();
			}
			output.collect(key, new IntWritable(sum));
		}
	} // Reduce 내부 클래스 종료
}