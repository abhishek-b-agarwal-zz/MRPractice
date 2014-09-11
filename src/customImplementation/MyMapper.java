package customImplementation;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MyMapper extends Mapper<MyKey, MyValue, Text, Text> {
        
          protected void map(MyKey key, MyValue value, Context context)
              throws java.io.IOException, InterruptedException {
        	  
            String sensor = key.getSensorType().toString();
            Text outputKey = new Text();
            Text outputValue = new Text();
            
            
            if(sensor.toLowerCase().equals("a")){
            	outputKey.set(sensor + " " + key.getTimestamp().toString() + " " + key.getStatus().toString());
            	outputValue.set(value.getValue1()+ " " + value.getValue2());
            	context.write(outputKey,outputValue);
            }
            		
          }  
}