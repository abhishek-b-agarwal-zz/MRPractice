package counters;

import java.io.IOException;

import java.util.Calendar;
import java.util.Date;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MyCounter {
	
	public static enum MONTH{
		DEC,
		JAN,
		FEB
	};
	
	
	
	public static class MyMapper extends Mapper<LongWritable,Text, Text, Text> {
        private Text out = new Text();
        protected void map(LongWritable key, Text value, Context context)
            throws java.io.IOException, InterruptedException {
        	String line = value.toString();
        	String[]  strts = line.split(",");
        	long lts = Long.parseLong(strts[1]);
        	
        	
        	Date time = new Date(lts);
        	
        	Calendar cal = Calendar.getInstance();
			cal.setTime(time);
        	int m = cal.get(Calendar.MONTH);
        	
        	//int m = time.getMonth();
        	
        	if(m==11){
        		context.getCounter(MONTH.DEC).increment(10);	
        	}
        	if(m==0){      	  	
      	  		context.getCounter(MONTH.JAN).increment(20);
        	}
        	if(m==1){
      	  		context.getCounter(MONTH.FEB).increment(30);
        	}
      	  	out.set("success");
      	  context.write(out,out);
        }  
}
	
	
  public static void main(String[] args) 
                  throws IOException, ClassNotFoundException, InterruptedException {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf);
    job.setJarByClass(MyCounter.class);
    job.setJobName("CounterTest");
    job.setNumReduceTasks(0);
    job.setMapperClass(MyMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    
    
    Path outputPath = new Path("/home/user/Documents/FoldersForMapReducePractice/Counters/Output");
    
    FileInputFormat.addInputPath(job, new Path("/home/user/Documents/FoldersForMapReducePractice/Counters/Input/input"));
    outputPath.getFileSystem(conf).delete(outputPath, true);
    FileOutputFormat.setOutputPath(job, outputPath);
    
    job.waitForCompletion(true);
    
    Counters counters = job.getCounters();
    
    Counter c1 = counters.findCounter(MONTH.DEC);
    System.out.println(c1.getDisplayName()+ " : " + c1.getValue());
    c1 = counters.findCounter(MONTH.JAN);
    System.out.println(c1.getDisplayName()+ " : " + c1.getValue());
    c1 = counters.findCounter(MONTH.FEB);
    System.out.println(c1.getDisplayName()+ " : " + c1.getValue());
    
  }
}
