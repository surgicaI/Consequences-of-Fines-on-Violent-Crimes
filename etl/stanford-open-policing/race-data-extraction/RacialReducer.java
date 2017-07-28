package extractor;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

public class RacialReducer extends Reducer<Text, IntArrayWritable, Text, Text> {

	@Override
	public void reduce(Text key, Iterable<IntArrayWritable> values,
			Context context) throws IOException, InterruptedException {

		long stopTotal = 0;
		long[] totals = new long[14];

		for (IntArrayWritable value : values) {
			stopTotal++;

			Writable[] inner = value.get();

			for (int i = 0; i < 14; i++) {
				Writable w = inner[i];
				totals[i] += ((IntWritable) w).get();
			}
		}
		
		StringBuilder sb = new StringBuilder(stopTotal + ",");
		
		for (int i = 0; i < 14; i++) {
			sb.append(totals[i]).append(",");
		}
		
		sb.setLength(sb.length() - 1);

		context.write(key, new Text(sb.toString()));
	}

}
