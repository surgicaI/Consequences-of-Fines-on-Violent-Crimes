package extractor;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class RacialRunner {
	
	private static final String[] states = new String[] { "az", "ca", "co",
		"ct", "fl", "ia", "il", "ma", "md", "mi", "mo", "ms", "mt", "nc",
		"nd", "ne", "nh", "nj", "nv", "oh", "or", "ri", "sc", "sd", "tn",
		"tx", "va", "vt", "wa", "wi", "wy" };
	
	public static void main(String[] args) {
		if (args.length != 2 && args.length != 3) {
			System.err.println("Incorrect program usage");
			System.exit(1);
		}
		
		String inputPath = args[args.length - 2];
		String outputPath = args[args.length - 1];
		
		
		try {			
			Configuration conf = new Configuration();
			conf.set("inputPath", inputPath);
			Job job = Job.getInstance(conf);
			job.setJarByClass(RacialRunner.class);
			job.setJobName("open policing racial data extractor");

			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(IntArrayWritable.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			
			job.setMapperClass(RacialMapper.class);
			job.setReducerClass(RacialReducer.class);
			
			for (String state : states) {
				FileInputFormat.addInputPath(job, new Path(inputPath + "/" + state + "-clean.csv"));
			}		
			
			FileOutputFormat.setOutputPath(job, new Path(outputPath));
			System.exit(job.waitForCompletion(true) ? 1 : 0);

		} catch (IOException | ClassNotFoundException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
