package test0121;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
//read ,1 
//a    ,1,1
//book ,1,1
//write,1

public class ScoretReducer  extends Reducer<Text,IntWritable,Text,IntWritable>{
	private IntWritable result = new IntWritable();
	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context) 
			throws IOException, InterruptedException {
		int sum = 0;
		for(IntWritable v : values) { 
			sum += v.get();
		}
		result.set(sum);
		context.write(key, result); 
	}

}