package merger.mappers;

import java.io.IOException;

import merger.TextArrayWritable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FBIMapper extends Mapper<LongWritable, Text, Text, TextArrayWritable>  {
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		if (key.get() != 0) {
			String[] values = value.toString().split(",");
			String state = values[0].substring(0, 2).toLowerCase();
			String county = values[1].toLowerCase();
			
			if (county.contains("county")) {
				county = county.replaceAll("county", "").trim();
			}
			
			context.write(new Text(state + " " + county), TextArrayWritable.getFromOtherDatasets(values));
			
		}
	}
}
