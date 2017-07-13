import java.io.*;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TaxesFinesSortedReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) 
               throws IOException, InterruptedException {

            for (Text value : values) {
                String fields = value.toString();
                context.write(key, new Text(fields));
            }
        } /* ~reduce */
} /* ~Reducer */
