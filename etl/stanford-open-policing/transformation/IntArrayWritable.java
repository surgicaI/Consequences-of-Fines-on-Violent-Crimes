package transformer;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;

public class IntArrayWritable extends ArrayWritable { 
	
	public IntArrayWritable() {
		super(IntWritable.class); 
	} 
	

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
        for (String s : super.toStrings())
        {
            sb.append(s).append("\t");
        }
        return sb.toString();
	}
}
