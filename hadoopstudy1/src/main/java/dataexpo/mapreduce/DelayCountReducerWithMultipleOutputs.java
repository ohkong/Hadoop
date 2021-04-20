package dataexpo.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class DelayCountReducerWithMultipleOutputs 
		extends Reducer<Text, IntWritable, Text, IntWritable> {
	private MultipleOutputs<Text, IntWritable> mos;
	private Text outputKey = new Text();	//reduce 출력키
	private IntWritable result = new IntWritable();	//reduce 츌력값
	
	@Override
	public void setup(Context context) throws IOException, InterruptedException{
		mos = new MultipleOutputs<Text, IntWritable>(context);//리듀서 실행 전에 mos 객체 설정
	}
	
	//key : D,1988,1
	public void reduce(Text key, Iterable<IntWritable> values, Context context) 
				throws IOException, InterruptedException {
		String[] colums = key.toString().split(",");	//콤마 구분자 분리
		outputKey.set(colums[1] + "," + colums[2]); //출력키 설정
		if(colums[0].equals("D")) {	//출발 지연
			int sum = 0;	//지연 횟수 합산
			for(IntWritable value : values) {
				sum += value.get();
			}
			result.set(sum);//출력값 설정
			mos.write("departure", outputKey, result);//출력 데이터 생성
		} else {//도착지연
			int sum = 0;	//지연 횟수 합산
			for(IntWritable value : values) {
				sum += value.get();
			}
			result.set(sum);//출력값 설정
			mos.write("arrival", outputKey, result);//출력 데이터 생성
		}
	}
	
	@Override
	public void cleanup(Context context) 
					throws IOException, InterruptedException {
		mos.close();
	}
		
}
