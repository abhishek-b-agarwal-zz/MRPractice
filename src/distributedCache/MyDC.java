package distributedCache;


import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MyDC {
	
	
	public static class MyMapper extends Mapper<LongWritable,Text, Text, Text> {
        
		
		private Map<String, String> abMap = new HashMap<String, String>();
				private Text outputKey = new Text();
				private Text outputValue = new Text();
				
		protected void setup(Context context) throws java.io.IOException, InterruptedException{
			//Path[] files = DistributedCache.getLocalCacheFiles(context.getConfiguration());
			URI[] files = context.getCacheFiles();
			String pathString = null;
			Path path = null;
			System.out.print("moving into setup\n");
			for (URI p : files) {
				pathString = p.getPath();
				path = new Path(pathString);
				if (path.getName().equals("abc.dat")) {
					BufferedReader reader = new BufferedReader(new FileReader(path.toString()));
					String line = reader.readLine();
					while(line != null) {
						String[] tokens = line.split("\t");
						String ab = tokens[0];
						String state = tokens[1];
						abMap.put(ab, state);
						line = reader.readLine();
					}
				}
			}
			if (abMap.isEmpty()) {
				throw new IOException("Unable to load Abbrevation data.");
			}
		}

		
        protected void map(LongWritable key, Text value, Context context)
            throws java.io.IOException, InterruptedException {
        	
        	System.out.print("moving into mapper");
        	String row = value.toString();
        	String[] tokens = row.split("\t");
        	String inab = tokens[0];
        	String state = abMap.get(inab);
        	outputKey.set(state);
        	outputValue.set(row);
      	  	context.write(outputKey,outputValue);
        }  
}
	
	
  public static void main(String[] args) 
                  throws IOException, ClassNotFoundException, InterruptedException {
    
	  Configuration conf = new Configuration();
    Job job = Job.getInstance(conf);
    job.setJarByClass(MyDC.class);
    job.setJobName("DCTest");
    job.setNumReduceTasks(0);
    
    try{
    	job.addCacheFile(new URI("/home/user/Documents/FoldersForMapReducePractice/DC/Cache/abc.dat"));
    
    }catch(Exception e){
    	System.out.println(e);
    }
    
    job.setMapperClass(MyMapper.class);
    
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    
    Path outputPath = new Path("/home/user/Documents/FoldersForMapReducePractice/DC/Output");
    
    FileInputFormat.addInputPath(job, new Path("/home/user/Documents/FoldersForMapReducePractice/DC/Input/input"));
    outputPath.getFileSystem(conf).delete(outputPath, true);
    FileOutputFormat.setOutputPath(job, outputPath);
    
    job.waitForCompletion(true);
    
    
  }
}
