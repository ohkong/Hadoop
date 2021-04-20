package dataexpo.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import dataexpo.Airline;
import dataexpo.DateKey;

public class DelayCountMapperWithDateKey 
		extends Mapper<LongWritable, Text, DateKey, IntWritable>{
	private final static IntWritable one = new IntWritable(1);
	private DateKey outputKey = new DateKey();
	
	public void map(LongWritable key, Text value, Context context) 
					throws IOException, InterruptedException{
		if(key.get() == 0) return;	//첫줄 skip
		Airline al = new Airline(value);
		if(al.isDepartureDelayAvailable()) {	//출발 지연 정보
			if(al.getDepartureDelayTime() > 0) {	//출발 지연 항공기
				outputKey.setYear("D," + al.getYear());
				outputKey.setMonth(al.getMonth());
				context.write(outputKey, one);
			} else if(al.getDepartureDelayTime() == 0) {//정시 출발
				context.getCounter(DelayCounters.scheduled_departure).increment(1);
			} else if(al.getDepartureDelayTime() < 0) {//조기출발
				context.getCounter(DelayCounters.early_departure).increment(1);
			}
		} else {//출발 지연 대상 아님
			context.getCounter(DelayCounters.not_available_departure).increment(1);
		}
		
		if(al.isArriveDelayAvailable()) {//도착지연 정보
			if(al.getArriveDelayTime() > 0) {//도착 지연 항공기
				outputKey.setYear("A," + al.getYear());
				outputKey.setMonth(al.getMonth());
				context.write(outputKey, one);
			} else if(al.getArriveDelayTime() == 0) {//정시 도착
				context.getCounter(DelayCounters.scheduled_arrival).increment(1);
			} else if(al.getArriveDelayTime() < 0) {//조기 도착
				context.getCounter(DelayCounters.early_arrival).increment(1);
			}
		} else {//도착 지연 대상 아님
			context.getCounter(DelayCounters.not_available_arrival).increment(1);
		}
	}

}