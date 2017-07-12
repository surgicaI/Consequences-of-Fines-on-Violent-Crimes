package transformer;

import java.io.IOException;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

public class TransformerReducer extends Reducer<Text, IntArrayWritable, Text, IntArrayWritable> {
	
	@Override
	public void reduce(Text key, Iterable<IntArrayWritable> values, Context context) throws IOException, InterruptedException {
		IntArrayWritable aw = new IntArrayWritable();
		IntWritable[] arr = new IntWritable[181];
		
		for (int i = 0; i < 181; i++) {
			arr[i] = new IntWritable(0);
		}
		
		for (ArrayWritable value : values) {
			Writable[] inner = value.get();
			
			for (int i = 0; i < 181; i++) {
				Writable w = inner[i];
				int count = arr[i].get();
				count += ((IntWritable) w).get();
				arr[i] = new IntWritable(count);
			}
		}
		
		aw.set(arr);
		context.write(key, aw);
	}

}
