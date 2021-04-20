package test0125;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class CarrDelayCountReducerWithMultipleOutputs extends Reducer<Text, IntWritable, Text, IntWritable> {
	private MultipleOutputs<Text, IntWritable> mos;
	private Text outputKey = new Text(); 
	private IntWritable result = new IntWritable(); 

	@Override
	public void setup(Context context) throws IOException, InterruptedException {
		mos = new MultipleOutputs<Text, IntWritable>(context); 
	}

//key : D,AA
	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		String[] colums = key.toString().split(",");
		outputKey.set(colums[1]);
		if (colums[0].equals("D")) {
			int sum = 0;
			for (IntWritable value : values) {
				sum += value.get();
			}
			result.set(sum);

			mos.write("departure", outputKey, result);

		} else { 
			int sum = 0; 
			for (IntWritable value : values) {
				sum += value.get();
			}
			result.set(sum); 
			mos.write("arrival", outputKey, result);
		}
	}
	@Override
	public void cleanup(Context context) throws IOException, InterruptedException {
		mos.close(); 
	}
}