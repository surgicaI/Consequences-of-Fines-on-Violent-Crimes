package extractor;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class RacialMapper extends Mapper<LongWritable, Text, Text, IntArrayWritable> {
	
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		if (key.get() != 0) {
			
			String state = ((FileSplit) context.getInputSplit()).getPath().getName().substring(0, 2).toLowerCase();
			Map<String, String> kvMap = getKVMap(context, value.toString());
			
			if (kvMap == null) return;
			
			if (kvMap.containsKey("driver_race") || kvMap.containsKey("driver_race_raw")) {
				IntArrayWritable aw = new IntArrayWritable();
				IntWritable[] values = new IntWritable[11];
				
				for (int i = 0; i < 11; i++) {
					values[i] = new IntWritable(0);
				}
				
				String contrabandFoundCol = "";
				boolean contrabandFound = false;
				
				if (kvMap.containsKey("contraband_found")) {
					contrabandFoundCol = kvMap.get("contraband_found");
				} else {
					values[6] = new IntWritable(1);
				}
				
				if (contrabandFoundCol.contains("t") || contrabandFoundCol.contains("y")) {
					contrabandFound = true;
				}
				
				String searchConductedCol = "";
				boolean searchConducted = false;
				
				if (kvMap.containsKey("search_conducted")) {
					searchConductedCol = kvMap.get("search_conducted");
				} else {
					values[10] = new IntWritable(1);
				}
				
				if (searchConductedCol.contains("t") || searchConductedCol.contains("y")) {
					searchConducted = true;
				}
								
				String race = "";
				
				if (kvMap.containsKey("driver_race")) {
					race = kvMap.get("driver_race");
				} else if (kvMap.containsKey("driver_race_raw")) {
					race = kvMap.get("driver_race_raw");
				}
				
				int cfi = -1;
				int sci = -1;
				
				if (race.contains("w")) {
					values[0] = new IntWritable(1);
					
					cfi = 3;
					sci = 7;
				} else if (race.contains("b")) {
					values[1] = new IntWritable(1);
					
					cfi = 4;
					sci = 8;
				} else if (race.contains("a")) {
					values[2] = new IntWritable(1);
					
					cfi = 5;
					sci = 9;
				} else {
					return;
				}
				
				if (contrabandFound) values[cfi] = new IntWritable(1);
				if (searchConducted) values[sci] = new IntWritable(1);
				
				aw.set(values);
				context.write(new Text(state), aw);
			}			
		}
	}
			

	private Map<String, String> getKVMap(Context context, String values) throws IOException {
		String state = ((FileSplit) context.getInputSplit()).getPath().getName().substring(0, 2).toLowerCase();
		Configuration config = context.getConfiguration();
		FileSystem fs = FileSystem.get(config);
		BufferedReader br= new BufferedReader(new InputStreamReader(fs.open(new Path("" + config.get("inputPath") + state + "-clean.csv"))));
		String headers = br.readLine();
		br.close();
		
		Map<String, String> map = new HashMap<>();
		String[] headersArr = headers.split(",");
		String[] valuesArr = values.split(",");

		if (valuesArr.length > headersArr.length) {
			return null;
		}

		for (int i = 0; i < valuesArr.length; i++) {
			if (!valuesArr[i].trim().equals("")) {
				map.put(headersArr[i].trim().toLowerCase(), valuesArr[i].trim().toLowerCase());
			}
		}

		return map;
	}
}
