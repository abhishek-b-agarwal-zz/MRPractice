package wordCount;

import java.io.IOException;

import junit.framework.TestCase;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Test;



public class TestWordCount extends TestCase{

	MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
	//ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;
	//MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;

	public void setUp()
	{
		new WordCountMapper();
	    //mapDriver = new MapDriver<LongWritable, Text, Text, IntWritable>();
	    mapDriver = MapDriver.newMapDriver(new WordCountMapper());
	 
	    /**
	    WordCountReducer reducer = new WordCountReducer();
	    reduceDriver = new ReduceDriver<Text, IntWritable, Text, IntWritable>();
	    reduceDriver.setReducer(reducer);
	 
	    mapReduceDriver = new MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable>();
	    mapReduceDriver.setMapper(mapper);
	    mapReduceDriver.setReducer(reducer);
	    **/
	}
	
	@Test
	public void testMapper() throws IOException
	{
	    mapDriver.withInput(new LongWritable(0), new Text("orange orange apple")).
	    withOutput(new Text("orange"), new IntWritable(1));
	    //mapDriver.withOutput(new Text("orange"), new IntWritable(1));
	    mapDriver.runTest();
	}
	
	/**
	@Test
	public void testReducer() throws IOException
	{
		List values = new ArrayList();
	    values.add(new IntWritable(1));
	    values.add(new IntWritable(1));
	    reduceDriver.withInput(new Text("orange"), values);
	    reduceDriver.withOutput(new Text("orange"), new IntWritable(2));
	    reduceDriver.runTest();
	}
	
	@Test
	public void testMapperReducer() throws IOException
	{
	    mapReduceDriver.addInput(new LongWritable(1), new Text("orange orange apple"));
	    mapReduceDriver.addOutput(new Text("orange"), new IntWritable(2));
	    mapReduceDriver.addOutput(new Text("apple"), new IntWritable(1));
	    mapReduceDriver.runTest();
	}
	**/
}
