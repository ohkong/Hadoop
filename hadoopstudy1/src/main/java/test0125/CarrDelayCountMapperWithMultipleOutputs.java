package test0125;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import dataexpo.Airline;
import dataexpo.mapreduce.DelayCounters;

public class CarrDelayCountMapperWithMultipleOutputs   extends Mapper<LongWritable, Text, Text, IntWritable> {
	  private final static IntWritable ONE = new IntWritable(1);
	  private Text outputKey = new Text();

	  public void map(LongWritable key, Text value, Context context)
	    throws IOException, InterruptedException {
		if(key.get() == 0) return;   //첫줄 skip
	    Airline airline= new Airline(value);
	    if (airline.isDepartureDelayAvailable()) { //출발지연정보
	      if (airline.getDepartureDelayTime() > 0) { //출발지연 항공기
	        outputKey.set("D," + airline.getUniqueCarrier());
	        context.write(outputKey, ONE);
	      } else if (airline.getDepartureDelayTime() == 0) { //정시출발
	        context.getCounter(DelayCounters.scheduled_departure).increment(1);
	      } else if (airline.getDepartureDelayTime() < 0) { //조기출발
	        context.getCounter(DelayCounters.early_departure).increment(1);
	      }
	    } else {  //출발지연 대상 아님.
	      context.getCounter(DelayCounters.not_available_departure).increment(1); //
	    }

	    if (airline.isArriveDelayAvailable()) { //도착지연정보
	      if (airline.getArriveDelayTime() > 0) { //도착 지연 항공기
	        outputKey.set("A,"+ airline.getUniqueCarrier());
	        context.write(outputKey, ONE);
	      } else if (airline.getArriveDelayTime() == 0) { //정시 도착
	        context.getCounter(
	          DelayCounters.scheduled_arrival).increment(1);
	      } else if (airline.getArriveDelayTime() < 0) { //조기 도착
	        context.getCounter(DelayCounters.early_arrival).increment(1);
	      }
	    } else {
	      context.getCounter(DelayCounters.not_available_arrival).increment(1);
	    }
	  }
	}