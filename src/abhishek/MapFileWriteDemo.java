package abhishek;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Options;

public class MapFileWriteDemo {
	private static final String[] DATA = {
	"One, two, buckle my shoe",
	"Three, four, shut the door",
	"Five, six, pick up sticks",
	"Seven, eight, lay them straight",
	"Nine, ten, a big fat hen"
	};
	public static void main(String[] args) throws IOException {
	String uri = "hdfs://localhost:9000/numbers.map";
	Path path = new Path(uri);
	Configuration conf = new Configuration();
	FileSystem fs = FileSystem.get(URI.create(uri), conf);
	IntWritable key = new IntWritable();
	
	Text value = new Text();
	
	MapFile.Writer writer = null;
	try {
	
	//writer = new MapFile.Writer(conf, fs, uri,
	//key.getClass(), value.getClass());
	for (int i = 0; i < 1024; i++) {
	key.set(i + 1);
	value.set(DATA[i % DATA.length]);
	writer.append(key, value);
	}
	} finally {
	IOUtils.closeStream(writer);
	}
	}
	}
