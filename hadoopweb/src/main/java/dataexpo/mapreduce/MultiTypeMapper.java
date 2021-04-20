package dataexpo.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import dataexpo.Airline;

public class MultiTypeMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
	private String workType;
	private final static IntWritable ONE = new IntWritable(1);
	private final static IntWritable DISTANCE = new IntWritable();
	private Text outkey = new Text();
	
	@Override
	public void setup(Context context) {
		workType = context.getConfiguration().get("workType");
	}
	
	@Override
	public void map(LongWritable key, Text value, Context context) 
					throws IOException, InterruptedException {
		if(key.get()==0) return;
		Airline al = new Airline(value);
		if(al.isDistanceAvailable() && al.getDistance() >0) {
			outkey.set("DI-"+al.getYear() + "-" + al.getMonth());
			DISTANCE.set(al.getDistance()); //운항 거리 설정
			context.write(outkey, DISTANCE);
		}
		switch(workType) {
		case "d" : //지연정보
			if(al.isDepartureDelayAvailable() && al.getDepartureDelayTime() > 0) {
				outkey.set("D-" + al.getYear() + "-" + al.getMonth());
				context.write(outkey, ONE);
			}
			if(al.isArriveDelayAvailable() && al.getArriveDelayTime() > 0) {
				outkey.set("A-" + al.getYear() + "-" + al.getMonth());
				context.write(outkey, ONE);
			}
			break;
		case "s" :
			if(al.isDepartureDelayAvailable() && al.getDepartureDelayTime() == 0) {
				outkey.set("D-" + al.getYear() + "-" + al.getMonth());
				context.write(outkey, ONE);
			}
			if(al.isArriveDelayAvailable() && al.getArriveDelayTime()== 0) {
				outkey.set("A-" + al.getYear() + "-" + al.getMonth());
				context.write(outkey, ONE);
			}
			break;
		case "e" : 
			if(al.isDepartureDelayAvailable() && al.getDepartureDelayTime() < 0) {
				outkey.set("D-" + al.getYear() + "-" + al.getMonth());
				context.write(outkey, ONE);
			}
			if(al.isArriveDelayAvailable() && al.getArriveDelayTime() < 0) {
				outkey.set("A-" + al.getYear() + "-" + al.getMonth());
				context.write(outkey, ONE);
			}
			break;
		}
	}
}
