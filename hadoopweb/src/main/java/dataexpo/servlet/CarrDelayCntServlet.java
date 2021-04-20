package dataexpo.servlet;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.TreeMap;

import javax.servlet.RequestDispatcher;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileAlreadyExistsException;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import dataexpo.mapreduce.CarrArrivalDelayMapper;
import dataexpo.mapreduce.CarrDelayReducer;
import dataexpo.mapreduce.CarrDepartureDelayMapper;


public class CarrDelayCntServlet extends HttpServlet {
	private static final long serialVersionUID = 1L;
       

    public CarrDelayCntServlet() {
        super();
    }


	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		response.setContentType("text/html;charset=UTF-8");
		String year = request.getParameter("year");
		String kbn = request.getParameter("kbn");
		String input = "D:/20200914/hadoophome/workspace/hadoopstudy1/infile/" + year + ".csv";
		String output = request.getSession().getServletContext().getRealPath("/")
								+ "output/carrdelay/" + year + "_" + kbn;
		Configuration conf = new Configuration();
		try {
			Job job = new Job(conf,"CarrDelayCntServlet");
			job.setJarByClass(this.getClass());
			if(kbn.equals("a")) {
				job.setMapperClass(CarrArrivalDelayMapper.class);
			} else {
				job.setMapperClass(CarrDepartureDelayMapper.class);
			}
			job.setReducerClass(CarrDelayReducer.class);
			
			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			FileInputFormat.addInputPath(job, new Path(input));
			FileOutputFormat.setOutputPath(job, new Path(output));
			job.waitForCompletion(true);
		}catch(FileAlreadyExistsException e) {
			System.out.println("기존 파일 존재 :" + output);
		}catch(Exception e) {
			e.printStackTrace();
		}
		String file = "part-r-00000";
		request.setAttribute("file", year);
		Path outFile = new Path(output + "/" + file);
		FileSystem fs = FileSystem.get(conf);
		BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(outFile)));
		//람다식 코딩 : 함수적 인터페이스(Functional Interface) => 추상메서드가 하나만 존재
		//Comparator 인터페이스
		Map<String, Integer> map = new TreeMap<String, Integer>();
		String line = null;
		while((line = br.readLine())!= null) {
			String[] v = line.split("\t");
			map.put(v[0].trim(), Integer.parseInt(v[1].trim()));
		}
		request.setAttribute("map", map);
		String view = request.getParameter("view");
		if(view == null)view="2";
		RequestDispatcher v = request.getRequestDispatcher("/dataexpo/dataexpo"+view+".jsp");
		v.forward(request, response);
	}


	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		doGet(request, response);
	}

}
