package test0125;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class MatroMapperWithMultipleOutputs extends Mapper<LongWritable, Text, Text, IntWritable> {
    private List<String> header = new ArrayList<>();
	public void map(LongWritable key, Text value, Context context) 
			                                       throws IOException, InterruptedException {
        if(key.get() == 0) {
        	for(String h : value.toString().replace("\"", "").split(",")) {
        		header.add(h);        	   
             }
    	    System.out.println(header.size() + ":" + header);
    	    return;
        }
  		String[] arr = value.toString().replace("\"", "").split(",");
  		
  		for(int i=3;i<arr.length-1;i+=2) { //승차정보
  			String outkey = "EN," + arr[1]+":"+header.get(i); 
  			IntWritable outputValue = new IntWritable(Integer.parseInt(arr[i]));
  			Text outputKey = new Text(outkey);
  			context.write(outputKey, outputValue);
 		}
  		for(int i=4;i<arr.length-1;i+=2) { //하차정보
  			String outkey = "EX," + arr[1]+":"+header.get(i); 
  			IntWritable outputValue = new IntWritable(Integer.parseInt(arr[i]));
  			Text outputKey = new Text(outkey);
  			context.write(outputKey, outputValue);
 		}
 	}
}