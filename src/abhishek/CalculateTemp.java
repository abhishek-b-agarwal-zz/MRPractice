package abhishek;

import java.io.IOException;
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



public class CalculateTemp { 
	
	/** 
	 * @author edureka!
	 * @interface Mapper
	 * <p>Map class is static and extends MapReduceBase and implements Mapper 
	 * interface having four hadoop generics type LongWritable, Text, IntWritable,
	 * Text
	 */
	
 
    public static class Map extends
            Mapper<LongWritable, Text, Text,IntWritable> { 

    	
      //Defining a local variable count of type IntWritable       
    	Text k= new Text(); 
 
      //Defining a local variable word of type Text 
 

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

            //StringTokenizer is breaking the record (line) according to the delimiter whitespace
            StringTokenizer tokenizer = new StringTokenizer(line," "); 

            //Iterating through all the tokens and forming the key value pair
            while (tokenizer.hasMoreTokens()) { 

            //The first token is going in year variable of type string
        	String year= tokenizer.nextToken();
        	k.set(year);

        	//Takes next token and removes all the whitespaces around it and then stores it in the string variable called temp
        	String temp= tokenizer.nextToken().trim();

        	//Converts string temp into integer v       	
        	int v = Integer.parseInt(temp); 

                //Sending to output collector which inturn passes the same to reducer
                //So in this case the output from mapper will be the length of a word and that word
             context.write(k,new IntWritable(v)); 
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
            Reducer<Text,IntWritable, Text, IntWritable> { 
 
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
    	
        @Override
        public void reduce(Text key, Iterable<IntWritable> values,Context context) 
              throws IOException, InterruptedException { 
        

        	//Defining a local variable sum of type int
        	int maxtemp=0;
            for (IntWritable a : values) { 
            
            //Defining a local variable temperature of type int which is taking all the temperature
            int temperature= Integer.parseInt(a.toString()); 
            
            	if(maxtemp<temperature)
            	{
            		maxtemp =temperature;
            	}
            } 
             
            
            //Dumping the output
            context.write(key, new IntWritable(maxtemp)); 
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
        job.setJarByClass(CalculateTemp.class); 
 
        //Setting configuration object with the Data Type of output Key and Value
        job.setOutputKeyClass(Text.class); 
        job.setOutputValueClass(IntWritable.class); 
       
        //Setting configuration object with the Data Type of output Key and Value of mapper
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
 
        //Providing the mapper and reducer class names
        job.setMapperClass(Map.class); 
        //job.setNumReduceTasks(0);
        job.setReducerClass(Reduce.class); 
 
        //Setting format of input and output
        job.setInputFormatClass(TextInputFormat.class); 
        job.setOutputFormatClass(TextOutputFormat.class); 
 
        //The hdfs input and output directory to be fetched from the command line
        FileInputFormat.setInputPaths(job, new Path("/home/user/Abhishek/Temperature")); 
        FileOutputFormat.setOutputPath(job, new Path("/home/user/Abhishek/outputMaxTemp")); 
       
        //Running the job
        //JobClient.runJob(conf);
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        }
    }

