package merger.mappers;

import java.io.IOException;

import merger.TextArrayWritable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class OPMapper extends Mapper<LongWritable, Text, Text, TextArrayWritable> {
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] values = value.toString().split("\t");
		String stateAndCounty = values[0].toLowerCase();
		
		String state = stateAndCounty.substring(0, 2);
		String county = stateAndCounty.substring(3);
		
		if (county.contains("county")) {
			county = county.replaceAll("county", "").trim();
		}
		
		context.write(new Text(state + " " + county), TextArrayWritable.getFromOPDataset(values));
	}
	
	
}
