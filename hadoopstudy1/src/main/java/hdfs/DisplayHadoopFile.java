package hdfs;

import java.io.BufferedReader;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


public class DisplayHadoopFile {
	public static void main(String[] args) throws Exception{
		String filepath = "infile/in.txt";//하둡서버존재 => local에 저장
		Path pt = new Path(filepath);
		Configuration conf = new Configuration();//하둡환경
		FileSystem fs = FileSystem.get(conf);//hdfs시스템. hadoop distribute file system
		BufferedReader br = new BufferedReader
				(new InputStreamReader(fs.open(pt),"UTF-8"));
		String line = null;
		while((line = br.readLine()) != null) {
			System.out.println(line);
		}
		br.close();
	}
}
