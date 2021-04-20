package test0121;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ScoreMapper extends Mapper <LongWritable,Text,Text,IntWritable>{
	private Text word = new Text();
	@Override
	public void map(LongWritable key, Text value, Context context) 
			                                      throws IOException, InterruptedException {
		String[] datas = value.toString().split(",");
		word.set(datas[0]);
		IntWritable score = new IntWritable(Integer.parseInt(datas[1]));
		context.write(word, score); 
     }
}