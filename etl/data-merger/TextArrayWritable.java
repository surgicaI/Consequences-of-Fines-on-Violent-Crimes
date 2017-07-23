package merger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class TextArrayWritable extends ArrayWritable implements Cloneable {
	
	public TextArrayWritable() {
		super(Text.class);
	} 
	
	
	public int size() {
		return get().length;
		
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
	
	public static TextArrayWritable getFromOPDataset(String[] values) {
		TextArrayWritable w = new TextArrayWritable();
		Text[] a = new Text[values.length - 1];
		
		
		for (int i = 1 ; i < values.length; i++) {
			a[i - 1] = new Text(values[i]);
		}
		
		w.set(a);
		return w;
	}
	
	public static TextArrayWritable getFromOtherDatasets(String[] values) {
		TextArrayWritable w = new TextArrayWritable();
		Text[] a = new Text[values.length - 2];
				
		for (int i = 2; i < values.length; i++) {
			a[i - 2] = new Text(values[i]);
		}
		
		w.set(a);
		return w;
	}
	
	public static TextArrayWritable getMerged(Iterable<TextArrayWritable> toMerge) throws CloneNotSupportedException {
		Iterator<TextArrayWritable> i = toMerge.iterator();
		List<TextArrayWritable> l = new ArrayList<>();
		while (i.hasNext()) {
			l.add((TextArrayWritable) i.next().clone());
		}
				
		if (l.size() < 2 || l.size() > 3) {
			return null;
		}
		
		Collections.sort(l, new Comparator<TextArrayWritable>() {
			@Override
			public int compare(TextArrayWritable o1, TextArrayWritable o2) {
				return o1.get().length - o2.get().length;
			}
		});
		
		if (l.get(0).size() > 10) { 
			return null;
		}
		
		if (l.get(1).size() > 100) {
			return null;
		}
		
		List<Writable> v = new ArrayList<>();
		
		for (TextArrayWritable t : l) {
			Writable[] a =  t.get();
			
			for (Writable at : a) {
				v.add(at);
			}
		}
		
		TextArrayWritable f = new TextArrayWritable();
		f.set(v.toArray(new Text[0]));
		return f;
	}
	
	@Override
	public Object clone() {
		TextArrayWritable clone = new TextArrayWritable();
		clone.set(get().clone());
		return clone;
		
	}


	public static TextArrayWritable getAggregated(
			Iterable<TextArrayWritable> values) {
		
		
		
		return null;
	}

}
