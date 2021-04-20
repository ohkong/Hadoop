package test0125;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class MatroReducerWithMultipleOutputs extends Reducer<Text, IntWritable, Text, IntWritable> {
	private MultipleOutputs<Text, IntWritable> mos;
	private Text outputKey = new Text(); 
	private IntWritable result = new IntWritable(); 

	@Override
	public void setup(Context context) throws IOException, InterruptedException {
		mos = new MultipleOutputs<Text, IntWritable>(context); 
	}

	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		String[] colums = key.toString().split(",");
		outputKey.set(colums[1]);
		if (colums[0].equals("EN")) {
			int sum = 0;
			for (IntWritable value : values) {
				sum += value.get();
			}
			result.set(sum);
			mos.write("enter", outputKey, result);
		} else { 
			int sum = 0; 
			for (IntWritable value : values) {
				sum += value.get();
			}
			result.set(sum); 
			mos.write("exit", outputKey, result);
		}
	}
	@Override
	public void cleanup(Context context) throws IOException, InterruptedException {
		mos.close(); 
	}
}
