package merger;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MergerReducer extends Reducer<Text, TextArrayWritable, Text, TextArrayWritable> {

	@Override
	public void reduce(Text key, Iterable<TextArrayWritable> values, Context context) throws IOException, InterruptedException {
		TextArrayWritable merged = null;
		try {
			merged = TextArrayWritable.getMerged(values);
		} catch (CloneNotSupportedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		if (merged != null) {
			context.write(key, merged);
		} else {
			System.out.println("Thrown out " + key);
		}
	}
}
