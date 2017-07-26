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
		long[] totals = new long[11];

		for (IntArrayWritable value : values) {
			stopTotal++;

			Writable[] inner = value.get();

			for (int i = 0; i < 11; i++) {
				Writable w = inner[i];
				totals[i] += ((IntWritable) w).get();
			}
		}

		long wc = (long) (((double) totals[3] / totals[0]) * 10000);
		long bc = (long) (((double) totals[4] / totals[1]) * 10000);
		long ac = (long) (((double) totals[5] / totals[2]) * 10000);

		long ws = (long) (((double) totals[7] / totals[0]) * 10000);
		long bs = (long) (((double) totals[8] / totals[1]) * 10000);
		long as = (long) (((double) totals[9] / totals[2]) * 10000);

		String cols = stopTotal + "," + totals[0] + "," + totals[1] + ","
				+ totals[2] + "," + totals[3] + "," + totals[4] + ","
				+ totals[5] + "," + totals[6] + "," + wc + "," + bc + "," + ac
				+ "," + totals[7] + "," + totals[8] + "," + totals[9] + ","
				+ totals[10] + "," + ws + "," + bs + "," + as;

		context.write(key, new Text(cols));
	}

}
