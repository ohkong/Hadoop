package dataexpo.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import dataexpo.Airline;

public class DelayCountMapperWithCounter extends Mapper<LongWritable,Text,Text,IntWritable>{
	private String workType;
	private final static IntWritable one = new IntWritable(1);
	private Text outputKey = new Text();
	
	@Override
	public void setup(Context context) throws IOException,InterruptedException{
		workType = context.getConfiguration().get("workType");	//departure
	}
	
	public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException{
		if(key.get()==0) return;	//입력파일의 첫줄이 header 정보이므로 처리 안함
		Airline parser = new Airline(value);
		if(workType.equals("departure")) {
			if(parser.isDepartureDelayAvailable()) {//출발지연 대상인 경우
				if(parser.getDepartureDelayTime() > 0) { //출발지연된 항공기
					outputKey.set(parser.getYear() + "," + parser.getMonth());//맵퍼의 키 설정
					context.write(outputKey, one);
				}else if(parser.getDepartureDelayTime() == 0) {//정시 출발인 경우
					context.getCounter(DelayCounters.scheduled_departure).increment(1);
				}else if(parser.getDepartureDelayTime() < 0) {//조기 출발인 경우
					context.getCounter(DelayCounters.early_departure).increment(1);
				}
			}else {//출발지연 대상이 아닌 경우
				context.getCounter(DelayCounters.not_available_departure).increment(1);
			}
		}else if(workType.equals("arrival")) {
			if(parser.isArriveDelayAvailable()) {//도착 지연 대상
				if(parser.getArriveDelayTime() > 0) {
					outputKey.set(parser.getYear() + "," + parser.getMonth());
					context.write(outputKey, one);
				}else if(parser.getArriveDelayTime() == 0) {//정시도착
					context.getCounter(DelayCounters.scheduled_arrival).increment(1);
				}else if(parser.getArriveDelayTime() < 0) {//일찍 도착
					context.getCounter(DelayCounters.early_arrival).increment(1);
				}
			}else {//유효하지 않은 도착 정보
				context.getCounter(DelayCounters.not_available_arrival).increment(1);
			}
		}
	}
}
