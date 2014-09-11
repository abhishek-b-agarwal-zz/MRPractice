package wordCount;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {

	public static void main(String args[]) throws IOException, InterruptedException, ClassNotFoundException {
        
		 
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
 
        job.setJarByClass(WordCount.class);
        job.setJobName("Word Count");
        
        Path outputPath = new Path("/home/user/Documents/FoldersForMapReducePractice/WordCount/Output");
        
        FileInputFormat.addInputPath(job, new Path("/home/user/Documents/FoldersForMapReducePractice/WordCount/Input/input"));
        outputPath.getFileSystem(conf).delete(outputPath, true);
        FileOutputFormat.setOutputPath(job, outputPath);
 
        job.setMapperClass(WordCountMapper.class);
        //job.setCombinerClass(BigramCountReducer.class);
        job.setCombinerClass(WordCountReducer.class);
        job.setReducerClass(WordCountReducer.class);
 
        //job.setMapOutputKeyClass(Text.class);
        //job.setMapOutputValueClass(IntWritable.class);
 
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
 
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
