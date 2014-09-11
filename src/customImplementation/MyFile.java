package customImplementation;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;


public class MyFile {

  public static void main(String[] args) 
                  throws Exception {
    /**
	  if (args.length != 2) {
      System.err.println("Usage: <input path> <output path>");
      System.exit(-1);
    }
    **/
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf);
    job.setJarByClass(MyFile.class);
    job.setJobName("CustomTest");
    job.setNumReduceTasks(0);
    job.setMapperClass(MyMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setInputFormatClass(MyInputFormat.class);
    Path outputPath = new Path("/home/user/Documents/FoldersForMapReducePractice/CustomFileInputFormat/Output");
    
    
    FileInputFormat.addInputPath(job, new Path("/home/user/Documents/FoldersForMapReducePractice/CustomFileInputFormat/Input/inputdata.txt"));
    outputPath.getFileSystem(conf).delete(outputPath, true);
    FileOutputFormat.setOutputPath(job, outputPath);
    
    job.waitForCompletion(true);
  }
}