package test0121;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;


public class MatroEx1 {
	public static void main(String[] args) throws IOException {
		JobConf conf = new JobConf(MatroEx1.class);
		conf.setJobName("MatroEx1");
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);
		conf.setMapperClass(Map.class);
//		conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(Reduce.class);
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		FileInputFormat.setInputPaths(conf, new Path("infile/14metro.csv"));
		FileOutputFormat.setOutputPath(conf, new Path("outfile/matro"));
		FileSystem hdfs = FileSystem.get(conf);
		if (hdfs.exists(new Path("outfile/matro"))) {
			hdfs.delete(new Path("outfile/matro"), true);
			System.out.println("기존 출력파일 삭제");
		}
		JobClient.runJob(conf); // 작업 시작
	}

	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
		private Text word = new Text();

		@Override
		public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			if(key.get() == 0) return; //첫번째 줄
			String[] lines = value.toString().split(",");
			word.set(lines[0]);
			//승차수
			output.collect(word, new IntWritable(Integer.parseInt(lines[2])));
		}
	} // Map 내부 클래스 종료

	public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
		@Override
		public void reduce(Text key, Iterator<IntWritable> value, OutputCollector<Text, IntWritable> output,
				Reporter reporter) throws IOException {
			int sum = 0;
			while (value.hasNext()) {
				sum += value.next().get();
			}
			output.collect(key, new IntWritable(sum));
		}
	} // Reduce 내부 클래스 종료

}