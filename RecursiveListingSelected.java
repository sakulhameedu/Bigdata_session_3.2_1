package hdfs;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class RecursiveListingSelected {
	public static void main(String[] args) {
		long start = 0;                      //starting value  
		long end = Long.MAX_VALUE;          //ending value
		
		if (args.length < 1 || args.length > 3) {
			System.out.println("Pass one, two or three arguments");
			System.exit(1);
		}
		else if (args.length == 2) {
			start = Long.parseLong(args[1]);
		}
		else if (args.length == 3) {
			start = Long.parseLong(args[1]);
			end = Long.parseLong(args[2]);
		}
		
		getStatus(new Path(args[0]), start, end); //getting file details
	}
	
	public static void getStatus(Path path, Long start, Long end) {
		try
		{
			Configuration conf = new Configuration();
			FileSystem fileSystem = FileSystem.get(path.toUri(), conf); //config file system
			FileStatus[] fileStatus=fileSystem.listStatus(path);
			
			for (FileStatus fStat : fileStatus) {
				//checking Directory with start and end date
				if (fStat.isDirectory() && fStat.getModificationTime() >= start && fStat.getModificationTime() <= end) {
					System.out.println("Directory: " + fStat.getPath());
					getStatus(fStat.getPath(), start, end);//get sub directory
				}
				//checking file with start and end date
				else if (fStat.isFile() && fStat.getModificationTime() >= start && fStat.getModificationTime() <= end) {
					System.out.println("File: " + fStat.getPath());
				}
				else if (fStat.isSymlink() && fStat.getModificationTime() >= start && fStat.getModificationTime() <= end) {
					System.out.println("Symlink: " + fStat.getPath());
				}
			}

		}
		catch (IOException e)
		{
            e.printStackTrace();
		}
	}
}
