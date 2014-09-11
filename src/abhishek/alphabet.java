package abhishek;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;



public class alphabet { 
	
	/** 
	 * @author edureka!
	 * @interface Mapper
	 * <p>Map class is static and extends MapReduceBase and implements Mapper 
	 * interface having four hadoop generics type LongWritable, Text, IntWritable,
	 * Text
	 */
	
 
    public static class Map extends
            Mapper<LongWritable, Text, IntWritable,Text> { 

    	
      //Defining a local variable count of type IntWritable       
    	private static IntWritable count ;
 
      //Defining a local variable word of type Text  
         private Text word = new Text(); 
 

     	//Mapper
     	
     	/**
     	 * @method map
     	 * <p>This method takes the input as text data type and splits the input into words.
     	 * Now the length of each word in the input is determined and key value pair is made.
     	 * This key value pair is passed to reducer.                                             
     	 * @method_arguments key, value, output, reporter
     	 * @return void
     	 */	
         
         /*
         * (non-Javadoc)
         * @see org.apache.hadoop.mapred.Mapper#map(java.lang.Object, java.lang.Object, org.apache.hadoop.mapred.OutputCollector, org.apache.hadoop.mapred.Reporter)
         */
        
        @Override
        public void map(LongWritable key, Text value, 
                Context context) 
                throws IOException,InterruptedException { 

        	//Converting the record (single line) to String and storing it in a String variable line
            String line = value.toString(); 

            //StringTokenizer is breaking the record (line) into words
            StringTokenizer tokenizer = new StringTokenizer(line); 
 
            //iterating through all the words available in that line and forming the key value pair	
            while (tokenizer.hasMoreTokens()) { 
            	
            	String thisH = tokenizer.nextToken();
            	
            	//finding the length of each token(word)
            	count= new IntWritable(thisH.length()); 
                word.set(thisH); 

                //Sending to output collector which inturn passes the same to reducer
                //So in this case the output from mapper will be the length of a word and that word
                context.write(count,word); 
            } 
        } 
    } 
 
    //Reducer
	
    /** 
     * @author edureka!
     * @interface Reducer
     * <p>Reduce class is static and extends MapReduceBase and implements Reducer 
     * interface having four hadoop generics type IntWritable,Text, IntWritable, IntWritable.
     */
 
    public static class Reduce extends 
            Reducer< IntWritable,Text, IntWritable, IntWritable> { 
 
    	/**
    	 * @method reduce
    	 * <p>This method takes the input as key and list of values pair from mapper, it does aggregation
    	 * based on keys and produces the final output.                                               
    	 * @method_arguments key, values, output, reporter	
    	 * @return void
    	 */	
    	
		 /*
		 * (non-Javadoc)
		 * @see org.apache.hadoop.mapred.Reducer#reduce(java.lang.Object, java.util.Iterator, org.apache.hadoop.mapred.OutputCollector, org.apache.hadoop.mapred.Reporter)
		 */
    	
        
        public void reduce(IntWritable key, Iterator<Text> values, 
                Context context) 
                throws IOException, InterruptedException { 

        	//Defining a local variable sum of type int
            int sum = 0; 

            /*
             * Iterates through all the values available with a key and add them together and give the final
             * result as the key and sum of its values.
             */

            while (values.hasNext()) { 
                values.next(); 
                sum ++;
            } 
            
            //Dumping the output
            context.write(key, new IntWritable(sum)); 
        } 
 
    } 

  //Driver

    /**
     * @method main
     * <p>This method is used for setting all the configuration properties.
     * It acts as a driver for map reduce code.
     * @return void
     * @method_arguments args
     * @throws Exception
     */
    
    public static void main(String[] args) throws Exception { 
 
    	//Creating a JobConf object and assigning a job name for identification purposes
    	//JobConf conf = new JobConf(alphabet.class);
    	Configuration conf = new Configuration();
    	Job job = Job.getInstance(conf);
        job.setJarByClass(alphabet.class); 
 
        //Setting configuration object with the Data Type of output Key and Value
        job.setOutputKeyClass(IntWritable.class); 
        job.setOutputValueClass(IntWritable.class); 
       
        //Setting configuration object with the Data Type of output Key and Value of mapper
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
 
        //Providing the mapper and reducer class names
        job.setMapperClass(Map.class); 
        job.setNumReduceTasks(0);
        //job.setReducerClass(Reduce.class); 
 
        //Setting format of input and output
        job.setInputFormatClass(TextInputFormat.class); 
        job.setOutputFormatClass(TextOutputFormat.class); 
 
        //The hdfs input and output directory to be fetched from the command line
        FileInputFormat.setInputPaths(job, new Path("/home/user/Abhishek/input")); 
        FileOutputFormat.setOutputPath(job, new Path("/home/user/Abhishek/output")); 
       
        //Running the job
        //JobClient.runJob(conf);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        }
    }

