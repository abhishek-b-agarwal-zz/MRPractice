package com.sampleproject;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class ImportReview {

	static String DELIMITER = ",";

	public static class Map extends Mapper<LongWritable, Text, Text, Text> {

		// 1, "one tow three" -> one, 1 and two ,1 and three,1
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException {

			XMLParser xP;
			try {
				xP = new XMLParser(value.toString());

				List<String> categories = xP.getCategory();
				List<String> reviewLists = xP.getReviews();
				int postive = 0;
				int negative = 0;

				for (String eachReview : reviewLists) {
					if (BadWords.isBad(eachReview)) {
						negative++;
					} else {
						postive++;
					}
				}

				for (String eachCat : categories) {
					String mapOutput = WordUtil.cleanWords(eachCat) + DELIMITER + xP.getHash() + DELIMITER + xP.getUrl() + DELIMITER + postive + DELIMITER + negative + DELIMITER + xP.getUsercount();
					//System.out.println(" output " + mapOutput);
					context.write(new Text("".trim()), new Text(mapOutput));
				}
			} catch (Exception e) {
				e.printStackTrace();
				throw new RuntimeException("");
			}


		}

	}

	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		conf.set("xmlinput.start", "<document>");
		conf.set("xmlinput.end", "</document>");
		Job job = Job.getInstance(conf,"importreview");
		
		job.setNumReduceTasks(0);
		job.setMapperClass(Map.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		// how the data will be read
		job.setInputFormatClass(XmlInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		Path outputPath = new Path("/home/user/Documents/FoldersForMapReducePractice/SampleProject/Output");
        
        //FileInputFormat.addInputPath(job, new Path("/home/user/Documents/FoldersForMapReducePractice/SampleProject/Input/input.xml"));
		//social-odp-2k9_annotations
		FileInputFormat.addInputPath(job, new Path("/home/user/Desktop/social-odp-2k9_annotations.xml"));
		outputPath.getFileSystem(conf).delete(outputPath, true);
        FileOutputFormat.setOutputPath(job, outputPath);

		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}
}

