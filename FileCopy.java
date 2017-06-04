package hdfs;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class FileCopy {
	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.out.println("Pass two arguments"); //Pass atleast two arguments 
			System.exit(1);
		}

		String localSrc = args[0];  //source destination
		String dst = args[1];  //destination 

		InputStream in = new BufferedInputStream(new FileInputStream(localSrc)); //path of input file 
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(dst), conf); //creating target file 
		OutputStream out = fs.create(new Path(dst)); //path of output file 
		
		IOUtils.copyBytes(in, out, 4096, true); //copy contents one file to another
	}
}
