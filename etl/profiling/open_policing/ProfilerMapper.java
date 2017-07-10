package profiler;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ProfilerMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		if (key.get() != 0) { //skip the header line
			Configuration config = context.getConfiguration();			
			context.write(new Text("stops"), new IntWritable(1));

			Map<String, String> kvMap = getKVMap(config.get("headers"), value.toString());

			if (kvMap != null) {
				if (kvMap.containsKey("stop_date")) {
					String stopDate = kvMap.get("stop_date");

					if (stopDate.startsWith("2013")) {
						context.write(new Text("stops in 2013"), new IntWritable(1));
						String month = stopDate.substring(5, 7);
						context.write(new Text("stops in month " + month), new IntWritable(1));

						if (kvMap.containsKey("stop_time")) {
							String stopTime = kvMap.get("stop_time");
							String hour = stopTime.substring(0, 2);
							context.write(new Text("stops at hour " + hour), new IntWritable(1));
						} else {
							context.write(new Text("stop_time-nodata"), new IntWritable(1));
						}

						emit(context, kvMap, "county_name");
						emit(context, kvMap, "driver_gender");
						emit(context, kvMap, "driver_age");
						emit(context, kvMap, "driver_race");
						emit(context, kvMap, "violation");
						emit(context, kvMap, "search_conducted");
						emit(context, kvMap, "search_type");
						emit(context, kvMap, "contraband_found");
						emit(context, kvMap, "stop_outcome");
						emit(context, kvMap, "is_arrested");
						emit(context, kvMap, "out_of_state");
						emit(context, kvMap, "drugs_related_stop");
						
						if (kvMap.containsKey("vehicle_type")) {
							String vehicleType = kvMap.get("vehicle_type");
							
							Pattern yearPattern = Pattern.compile("\\d{4}");
							Matcher yearMatcher = yearPattern.matcher(vehicleType);
							boolean found = false;
							
							while (yearMatcher.find()) {
								int year = Integer.valueOf(yearMatcher.group(0));
								
								if (year >= 1980 && year <= 2014) {
									context.write(new Text("vehicle_year-" + year), new IntWritable(1));
									found = true;
									break;
								}
							}
							
							if (!found) {
								context.write(new Text("vehicle_year-nodata"), new IntWritable(1));
							}
							
							
						} else {
							context.write(new Text("vehicle_type-nodata"), new IntWritable(1));
						}

					}
				} else {
					context.write(new Text("stop_date-nodata"), new IntWritable(1));
				}

			} else {
				context.write(new Text("bad-row"), new IntWritable(1));
			}
		}


	}

	private void emit(Context context, Map<String, String> kvMap, String key) throws IOException, InterruptedException {
		if (kvMap.containsKey(key)) {
			String value = kvMap.get(key);
			context.write(new Text(key + "-" + value), new IntWritable(1));
		} else {
			context.write(new Text(key + "-nodata"), new IntWritable(1));
		}
	}

	private Map<String, String> getKVMap(String headers, String values) {
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
