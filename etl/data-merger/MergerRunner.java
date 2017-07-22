package merger;

import java.io.IOException;

import merger.mappers.CensusMapper;
import merger.mappers.FBIMapper;
import merger.mappers.OPMapper;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MergerRunner {

	private static final String[] states = new String[] { "AZ", "CA", "CO",
			"CT", "FL", "IA", "IL", "MA", "MD", "MI", "MO", "MS", "MT", "NC",
			"ND", "NE", "NH", "NJ", "NV", "OH", "OR", "RI", "SC", "SD", "TN",
			"TX", "VA", "VT", "WA", "WI", "WY" };

	public static void main(String[] args) {
		if (args.length != 4 && args.length != 5) {
			System.err.println("Incorrect program usage");
			System.exit(1);
		}

		String fbiData = args[args.length - 4];
		String censusData = args[args.length - 3];
		String opDataPath = args[args.length - 2];
		String outputPath = args[args.length - 1];

		try {
			Job job = new Job();
			job.setJarByClass(MergerRunner.class);
			job.setJobName("fine-crime data merger");

			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(TextArrayWritable.class);
			job.setReducerClass(MergerReducer.class);

			MultipleInputs.addInputPath(job, new Path(fbiData),
					TextInputFormat.class, FBIMapper.class);
			MultipleInputs.addInputPath(job, new Path(censusData),
					TextInputFormat.class, CensusMapper.class);

			for (String state : states) {
				MultipleInputs.addInputPath(job, new Path(opDataPath + "/"
						+ state), TextInputFormat.class, OPMapper.class);
			}
			
			FileOutputFormat.setOutputPath(job, new Path(outputPath));
			
			System.exit(job.waitForCompletion(true) ? 1 : 0);

		} catch (IOException | ClassNotFoundException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
