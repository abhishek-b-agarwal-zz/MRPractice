package wordCount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
 
 
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable>
{   
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
    {
        String line = value.toString();
        
        //Split the line into words
        for(String word: line.split("\\W+"))
        {
            //Make sure that the word is legitimate
            if(word.length() > 0)
            {
                //Emit the word as you see it
                context.write(new Text(word), new IntWritable(1));
            }
        }
    }
}
