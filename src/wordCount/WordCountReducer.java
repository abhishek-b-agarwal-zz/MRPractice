package wordCount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
 
 
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
 
    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
    {
        //Initializing the word count to 0 for every key
        int count=0;
        
        for(IntWritable value: values)
        {
            //Adding the word count counter to count
            count += value.get();
        }
        
        //Finally write the word and its count
        context.write(key, new IntWritable(count));
    }
}
