package hdfs;

import java.io.IOException;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class WriteHadoopFile1 {
	public static void main(String[] args) throws IOException {
		String filepath = "outfile/out.txt";
		Path pt = new Path(filepath);
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		System.out.println("하둡에 저장할 문자를 입력하세요");
		Scanner scan = new Scanner(System.in);
		FSDataOutputStream out = fs.create(pt);
		while(true) {
			String console = scan.nextLine();
			if(console.equals("exit"))break;
			out.writeUTF(console + "\n");
			out.flush();
		}
		out.close();
	}

}
