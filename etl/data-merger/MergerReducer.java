package merger;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MergerReducer extends
		Reducer<Text, TextArrayWritable, Text, TextArrayWritable> {

	@Override
	public void reduce(Text key, Iterable<TextArrayWritable> values,
			Context context) throws IOException, InterruptedException {
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

	@Override
	public void setup(Context context) throws IOException, InterruptedException {
		TextArrayWritable headers = new TextArrayWritable();

		String k = "population,fine revenue,tax revenue,fines/taxes,tax p/c,fines p/c,";
		
		String i = "agency,months reporting,population coverage,violent crime total,"
				+ "murder and nonnegligent manslaughter,legacy rape,revised rape,robbery,aggravated assault"
				+ ",property crime total,burglary,larceny-theft,motor vehicle theft,violent crime rate,"
				+ "murder and nonnegligent manslaughter rate,Legacy rape rate,revised rape rate,robbery rate,"
				+ "aggravated assault rate,property crime rate,burglary rate,larceny-theft rate,motor vehicle theft rate,";
		
		String a = "stops jan,stops feb,stops mar,stops apr,stops may,stops jun,stops jul,stops aug,stops sep,stops oct,stops nov,stops dec,"
				+ "stops 00,stops 01,stops 02,stops 03,stops 04,stops 05,stops 06,stops 07,stops 08,stops 09,stops 10,stops 11,"
				+ "stops 12,stops 13,stops 14,stops 15,stops 16,stops 17,stops 18,stops 19,stops 20,stops 21,stops 22,stops 23,"
				+ "male,female,unknown/other,white,black,hispanic,asian,unknown/other,age<15,age=15,age=16,age=17,age=18,age=19,"
				+ "age=20,age=21,age=22,age=23,age=24,age=25,age=26,age=27,age=28,age=29,age=30,"
				+ "age=31,age=32,age=33,age=34,age=35,age=36,age=37,age=38,age=39,age=40,"
				+ "age=41,age=42,age=43,age=44,age=45,age=46,age=47,age=48,age=49,age=50,"
				+ "age=51,age=52,age=53,age=54,age=55,age=56,age=57,age=58,age=59,"
				+ "age=60,age=61,age=62,age=63,age=64,age=65,age=66,age=67,age=68,age=69,age=70,"
				+ "age=71,age=72,age=73,age=74,age=75,age=76,age=77,age=78,age=79,age=80,"
				+ "age=81,age=82,age=83,age=84,age=85,age>85,age unknown,"
				+ "car=1980,car=1981,car=1982,car=1983,car=1984,car=1985,car=1986,car=1987,car=1988,car=1989,"
				+ "car=1990,car=1991,car=1992,car=1993,car=1994,car=1995,car=1996,car=1997,car=1998,car=1999,"
				+ "car=2000,car=2001,car=2002,car=2003,car=2004,car=2005,car=2006,car=2007,car=2008,car=2009,"
				+ "car=2010,car=2011,car=2012,car=2013,car=2014,car unknown,speed,dui,license,paperwork,"
				+ "stop sign/traffic light violation,safe movement,unknown/other,search conducted,search not conducted,"
				+ "unknown if search conducted,consented,k9,frisk,incident to arrest,other/unknown search,contraband found,"
				+ "contraband not found,unknown if contraband found,verbal warning,written warning,arrest/citation,unknown outcome,"
				+ "was arrested,not arrested,unknown if arrested,out-of-state,in-state,unknown state,drugs related,not drugs related,"
				+ "unknown if drugs related";
				
		String[] h = (k + i + a).split(",");
		Text[] arr = new Text[h.length];
		
		for (int j = 0; j < arr.length; j++) {
			arr[j] = new Text(h[j]);
		}
		

		headers.set(arr);
		context.write(new Text("location"), headers);
	}
}
