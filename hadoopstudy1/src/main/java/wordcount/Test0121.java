package wordcount;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

//hadoop2 버전으로 코딩. Mapper, Reducer를 내부클래스로 코딩
public class Test0121 {
	public static void main(String[] args) throws IOException{
		JobConf conf = new JobConf(Test0121.class);//작업 + 환경설정
		conf.setJobName("Test0121");
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);
		conf.setMapperClass(Map.class);//맴퍼 설정
		conf.setCombinerClass(Reduce.class);//컴바이너 설정 : 맵퍼리듀서사이의 리듀서의 기능을 통해 성능향상.
		conf.setReducerClass(Reduce.class);//리듀서 설정
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		//여러개의 입력파일 설정
		FileInputFormat.setInputPaths(conf, new Path("infile/score.txt"));//입력파일 설정
		//출력파일 설정
		FileOutputFormat.setOutputPath(conf, new Path("outfile/tot"));
		FileSystem hdfs = FileSystem.get(conf);
		if(hdfs.exists(new Path("outfile/tot"))) {
			hdfs.delete(new Path("outfile/tot"),true);
			System.out.println("기존 출력파일 삭제");
		}
		JobClient.runJob(conf);//작업시작
	}
	
	public static class Map extends MapReduceBase 
					implements Mapper<LongWritable, Text, Text, IntWritable>{
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		@Override
		public void map(LongWritable key, Text value,
					OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			String line = value.toString();
			StringTokenizer itr = new StringTokenizer(line);
			while(itr.hasMoreElements()) {
				word.set(itr.nextToken());
				String name = word.toString().split(",")[0];
				int score = Integer.parseInt(word.toString().split(",")[1]);
				Text namevalue = new Text();
				IntWritable scorevalue = new IntWritable(score);
				namevalue.set(name);
				output.collect(namevalue, scorevalue);
			}
		}
	}	//Map 내부 클래스 종료

	public static class Reduce extends MapReduceBase
			implements Reducer<Text, IntWritable, Text, IntWritable>{

		@Override
		public void reduce(Text key, Iterator<IntWritable> value, 
				OutputCollector<Text, IntWritable> output,
						Reporter reporter) throws IOException {
			int sum = 0;
			while(value.hasNext()) {
				sum += value.next().get();
			}
			output.collect(key, new IntWritable(sum));
		}
	}//Reuce 내부 클래스 종료
}
