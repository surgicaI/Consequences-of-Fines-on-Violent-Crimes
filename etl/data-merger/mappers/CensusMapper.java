package merger.mappers;

import java.io.IOException;

import merger.TextArrayWritable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CensusMapper extends
		Mapper<LongWritable, Text, Text, TextArrayWritable> {

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();

		if (line.startsWith("county")) {
			line = line.substring(7);
			String[] values = line.split(",");
			String county = values[0].toLowerCase();
			String state = values[1].toLowerCase();

			if (county.contains("county")) {
				county = county.replaceAll("county", "").trim();
			}

			state = getStateCode(state);
			
			context.write(new Text(state + " " + county), TextArrayWritable.getFromOtherDatasets(values));
		}
	}

	private String getStateCode(String state) {
		switch (state) {
		case "alabama":
			return "al";
		case "alaska":
			return "ak";
		case "arizona":
			return "az";
		case "arkansas":
			return "ar";
		case "california":
			return "ca";
		case "colorado":
			return "co";
		case "connecticut":
			return "ct";
		case "delaware":
			return "de";
		case "florida":
			return "fl";
		case "georgia":
			return "ga";
		case "hawaii":
			return "hi";
		case "idaho":
			return "id";
		case "illinois":
			return "il";
		case "indiana":
			return "in";
		case "iowa":
			return "ia";
		case "kansas":
			return "ks";
		case "kentucky":
			return "ky";
		case "louisiana":
			return "la";
		case "maine":
			return "me";
		case "maryland":
			return "md";
		case "massachusetts":
			return "ma";
		case "michigan":
			return "mi";
		case "minnesota":
			return "mn";
		case "mississippi":
			return "ms";
		case "missouri":
			return "mo";
		case "montana":
			return "mt";
		case "nebraska":
			return "ne";
		case "nevada":
			return "nv";
		case "new hampshire":
			return "nh";
		case "new jersey":
			return "nj";
		case "new mexico":
			return "nm";
		case "new york":
			return "ny";
		case "north carolina":
			return "nc";
		case "north dakota":
			return "nd";
		case "ohio":
			return "oh";
		case "oklahoma":
			return "ok";
		case "oregon":
			return "or";
		case "pennsylvania":
			return "pa";
		case "rhode island":
			return "ri";
		case "south carolina":
			return "sc";
		case "south dakota":
			return "sd";
		case "tennessee":
			return "tn";
		case "texas":
			return "tx";
		case "utah":
			return "ut";
		case "vermont":
			return "vt";
		case "virginia":
			return "va";
		case "washington":
			return "wa";
		case "west virginia":
			return "wv";
		case "wisconsin":
			return "wi";
		case "wyoming":
			return "wy";
		}

		return "invalid";
	}

}
