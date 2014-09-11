package abhishek;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

public class ListStatus {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub

		String uri = "hdfs://localhost:9000/";
		
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(uri), conf);
		
		FileStatus[] status = fs.listStatus(new Path(uri));
		Path[] listedPaths = FileUtil.stat2Paths(status);
		for (Path p : listedPaths)
			System.out.println(p);
	}

}
