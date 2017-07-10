package profiler;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ProfilerRunner {
	
	public static void main(String[] args) {
		if (args.length != 2 && args.length != 3) {
			System.err.println("Incorrect program usage");
			System.exit(1);
		}
		
		String inputPath = args[args.length - 2];
		String outputPath = args[args.length -1];
		
		try {
			Configuration conf = new Configuration();
			conf.set("headers", getHeaders(new Path("" + inputPath), conf));
			
			
			Job job = new Job(conf);
			job.setJarByClass(ProfilerRunner.class);
			job.setJobName("Stanford OpenPolicing Data Profiler");
			
			FileInputFormat.addInputPath(job, new Path(inputPath));
			FileOutputFormat.setOutputPath(job, new Path(outputPath));
			
			job.setMapperClass(ProfilerMapper.class);
			job.setReducerClass(ProfilerReducer.class);
			
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(IntWritable.class);
			
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(LongWritable.class);
			
			System.exit(job.waitForCompletion(true) ? 1 : 0);
		} catch (IOException | ClassNotFoundException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private static String getHeaders(Path filePath, Configuration conf) throws IOException {
		FileSystem fs = FileSystem.get(conf);
		BufferedReader br= new BufferedReader(new InputStreamReader(fs.open(filePath)));
		String ret = br.readLine();
		br.close();
		return ret;
	}

}
