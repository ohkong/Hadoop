package dataexpo.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import dataexpo.DateKey;

public class DelayCountReducerWithDateKey 
			extends Reducer<DateKey, IntWritable, DateKey, IntWritable> {
	private MultipleOutputs<DateKey, IntWritable> mos;
	private DateKey outputKey = new DateKey();	//reduce 출력키
	private IntWritable result = new IntWritable();	//reduce 츌력값
	
	@Override
	public void setup(Context context) throws IOException, InterruptedException{
		mos = new MultipleOutputs<DateKey, IntWritable>(context);//리듀서 실행 전에 mos 객체 설정
	}
	
	//key : D,1988,1
	public void reduce(DateKey key, Iterable<IntWritable> values, Context context) 
				throws IOException, InterruptedException {
		String[] colums = key.getYear().split(",");	//콤마 구분자 분리
		outputKey.setYear(key.getYear().substring(2));; //출력키 설정
		outputKey.setMonth(key.getMonth());
		int sum = 0;	//지연 횟수 합산
		for(IntWritable value : values) {
			sum += value.get();
		}
		result.set(sum);//출력값 설정
		if(colums[0].equals("D")) {
			mos.write("departure", outputKey, result);
		} else {
			mos.write("arrival",outputKey,result);
		}
	}
	
	@Override
	public void cleanup(Context context) throws IOException, InterruptedException{
		mos.close();
	}
}