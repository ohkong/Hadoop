package dataexpo.servlet;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import javax.servlet.RequestDispatcher;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
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
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import dataexpo.mapreduce.CarrMultiTypeMapper;
import dataexpo.mapreduce.CarrMultiTypeReducer;



@WebServlet("/CarrMultiServlet2")	//web.xml의 servlet 태그 대체
public class CarrMultiServlet2 extends HttpServlet {
	private static final long serialVersionUID = 1L;
       
    public CarrMultiServlet2() {
        super();
    }

	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		response.setContentType("text/html;charset=UTF-8");
		String year = request.getParameter("year");
		String kbn = request.getParameter("kbn");
		String input = "D:/20200914/hadoophome/workspace/hadoopstudy1/infile/" + year + ".csv";
		String output = request.getSession().getServletContext().getRealPath("/")
								+ "/carrmonmultiout" + year + "_" + kbn;
		Configuration conf = new Configuration();
		conf.set("workType", kbn);
		try {
			Job job = new Job(conf,"CarrMultiServlet2");
			FileInputFormat.addInputPath(job, new Path(input));
			FileOutputFormat.setOutputPath(job, new Path(output));
			job.setJarByClass(this.getClass());
			job.setMapperClass(CarrMultiTypeMapper.class);
			job.setReducerClass(CarrMultiTypeReducer.class);
			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			
			MultipleOutputs.addNamedOutput(job, "departure",
						TextOutputFormat.class, Text.class, IntWritable.class);
			MultipleOutputs.addNamedOutput(job, "arrival",
					TextOutputFormat.class, Text.class, IntWritable.class);
			MultipleOutputs.addNamedOutput(job, "distance",
					TextOutputFormat.class, Text.class, IntWritable.class);
			
			job.waitForCompletion(true);
		}catch(FileAlreadyExistsException e) {
			System.out.println("기존 파일 존재 :" + output);
		}catch(Exception e) {
			e.printStackTrace();
		}
		
		String[] files = {"departure-r-00000","arrival-r-00000","distance-r-00000"};
		List<Map<String,Integer>> list = new ArrayList<Map<String,Integer>>();
		request.setAttribute("file", year);
		for(String f : files) {
			Path out = new Path(output + "/" + f);
			FileSystem fs = FileSystem.get(conf);
			BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(out)));
			Map<String, Integer> map = new TreeMap<String, Integer>
			((o1,o2)->Integer.parseInt(o1.split("-")[1])-Integer.parseInt(o2.split("-")[1]));
			String line = null;
			while((line = br.readLine())!= null) {
				String[] v = line.split("\t");
				int cnt = Integer.parseInt(v[1].trim());
				map.put(v[0].trim(), cnt);
			}
			list.add(map);
		}
		request.setAttribute("list", list);
		String jsp = "/dataexpo/dataexpo6.jsp";
		RequestDispatcher dispatcher = request.getRequestDispatcher(jsp);
		dispatcher.forward(request, response);
	}

	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		doGet(request, response);
	}

}
