package dataexpo.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import dataexpo.Airline;

public class MultiDelayMapper 
			extends Mapper<LongWritable, Text, Text, IntWritable> {
	private final static IntWritable one = new IntWritable(1);
	private Text outputKey = new Text();

	@Override
	public void map(LongWritable key, Text value, Context context) 
							throws IOException, InterruptedException {
		if (key.get() == 0)
			return;
		Airline air = new Airline(value);
		if (air.isDepartureDelayAvailable() && air.getDepartureDelayTime() > 0) {
			outputKey.set("D-" + air.getYear() + "-" + air.getMonth());
			context.write(outputKey, one);
		}
		if(air.isArriveDelayAvailable() && air.getArriveDelayTime() > 0){
			outputKey.set("A-" +air.getYear() + "-" + air.getMonth());
			context.write(outputKey, one);
		}
	}
}