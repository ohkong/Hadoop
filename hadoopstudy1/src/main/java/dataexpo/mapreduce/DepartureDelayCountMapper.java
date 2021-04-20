package dataexpo.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import dataexpo.Airline;

public class DepartureDelayCountMapper extends 
			Mapper<LongWritable,Text,Text,IntWritable>{
	private final static IntWritable outputValue = new IntWritable(1);
	private Text outputKey = new Text();
	
	public void map(LongWritable key, Text value, Context context) 
							throws IOException, InterruptedException{
		if(key.get() == 0) return;
		Airline air = new Airline(value);
		outputKey.set(air.getYear() + "," + air.getMonth());
		if(air.isDepartureDelayAvailable() && air.getDepartureDelayTime()> 0) {
			context.write(outputKey, outputValue);
		}
	}
}
