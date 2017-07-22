package transformer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TransformerRunner {
	
	private static final String[] states = new String[] { "AZ", "CA", "CO",
		"CT", "FL", "IA", "IL", "MA", "MD", "MI", "MO", "MS", "MT", "NC",
		"ND", "NE", "NH", "NJ", "NV", "OH", "OR", "RI", "SC", "SD", "TN",
		"TX", "VA", "VT", "WA", "WI", "WY" };
	
	public static void main(String[] args) {
		if (args.length != 2 && args.length != 3) {
			System.err.println("Incorrect program usage");
			System.exit(1);
		}
		
		String inputPath = args[args.length - 2];
		String outputPath = args[args.length - 1];
		
		
		try {			
			for (String state : states) {
				Configuration conf = new Configuration();
				conf.set("headers", getHeaders(new Path("" + inputPath + state + "-clean.csv"), conf));
				conf.set("state", state);
				
				Job job = new Job(conf);
				job.setJarByClass(TransformerRunner.class);
				
				job.setMapperClass(TransformerMapper.class);
				job.setReducerClass(TransformerReducer.class);
				
				job.setOutputKeyClass(Text.class);
				job.setOutputValueClass(IntArrayWritable.class);
				
				job.setJobName("Transforming " + state);
				
				FileInputFormat.addInputPath(job, new Path(inputPath + state + "-clean.csv"));
				FileOutputFormat.setOutputPath(job, new Path(outputPath + state));
				
				job.waitForCompletion(true);
			}
												
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
