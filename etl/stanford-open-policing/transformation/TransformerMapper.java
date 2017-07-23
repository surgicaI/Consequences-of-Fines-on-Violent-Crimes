package transformer;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TransformerMapper extends Mapper<LongWritable, Text, Text, IntArrayWritable> {
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		if (key.get() != 0) {
			Configuration config = context.getConfiguration();			
			Map<String, String> kvMap = getKVMap(config.get("headers"), value.toString());
			
			if (kvMap == null) return;
			
			if (kvMap.containsKey("stop_date")) {
				String stopDate = kvMap.get("stop_date");
				
				if (stopDate.contains("2013")) {
					if (kvMap.containsKey("county_name") || kvMap.containsKey("district")) {
						IntArrayWritable aw = new IntArrayWritable();
						IntWritable[] values = new IntWritable[185];
						
						for (int i = 0; i < 185; i++) {
							values[i] = new IntWritable(0); //null causes errors
						}
						
						int i = 0;
						
						/*	
						 * 	values[0]  ==  JAN
						 * 	values[1]  ==  FEB
						 * 	values[2]  ==  MAR
						 * 	values[3]  ==  APR
						 * 	values[4]  ==  MAY
						 * 	values[5]  ==  JUN
						 * 	values[6]  ==  JUL
						 * 	values[7]  ==  AUG
						 * 	values[8]  ==  SEP
						 * 	values[9]  ==  OCT
						 * 	values[10] ==  NOV
						 * 	values[11] ==  DEC
						 */
						
						int month = Integer.parseInt(stopDate.substring(5, 7));
						values[i + (month - 1)] = new IntWritable(1);
						i += 12;
						
						/*
						 * 	values[12]	==	midnight
						 * 	values[13]	==	1am
						 * 	values[14]	==	2am
						 * 	values[15]	==	3am
						 * 	values[16]	==	4am
						 * 	values[17]	==	5am
						 * 	values[18]	==	6am
						 * 	values[19]	==	7am
						 * 	values[20]	==	8am
						 * 	values[21]	==	9am
						 * 	values[22]	==	10am
						 * 	values[23]	==	11am
						 * 	values[24]	==	midday
						 * 	values[25]	==	1pm
						 * 	values[26]	==	2pm
						 * 	values[27]	==	3pm
						 * 	values[28]	==	4pm
						 * 	values[29]	==	5pm
						 * 	values[30]	==	6pm
						 * 	values[31]	==	7pm
						 * 	values[32]	==	8pm
						 * 	values[33]	==	9pm
						 * 	values[34]	==	10pm
						 * 	values[35]	==	11pm
						 */
						
						if (kvMap.containsKey("stop_time")) {
							String stopTime = kvMap.get("stop_time");
							int hour = Integer.parseInt(stopTime.substring(0, 2));
							values[i + hour] = new IntWritable(1);
						}
						i += 24;
						
						String gender = "";
						
						/*
						 * 	values[36]	==	male
						 *  values[37]	==	female
						 *  values[38]	==	unknown/other
						 */
						
						if (kvMap.containsKey("driver_gender")) {
							gender = kvMap.get("driver_gender");
						} else if (kvMap.containsKey("driver_gender_raw")) {
							gender = kvMap.get("driver_gender_raw");
						}
						
						if (gender.equals("m") || gender.equals("male")) {
							values[i] = new IntWritable(1);
						} else if (gender.equals("f") || gender.equals("female")) {
							values[i + 1] = new IntWritable(1);
						} else if ((gender.contains("ma") && !gender.contains("fe")) ||( gender.contains("man") && !gender.contains("wo"))) {
							values[i] = new IntWritable(1);
						} else if (gender.contains("fe") || gender.contains("wo")) {
							values[i + 1] = new IntWritable(1);
						} else {
							values[i + 2] = new IntWritable(1);
						}
						
						String race = "";
						
						i += 3;
						
						/*
						 *   values[39]	==	White
						 *   values[40]	==	Black
						 *   values[41]	==	Hispanic
						 *   values[42]	==	Asian
						 *   values[43]	==	unknown/other
						 */
						
						if (kvMap.containsKey("driver_race")) {
							race = kvMap.get("driver_race");
						} else if (kvMap.containsKey("driver_race_raw")) {
							race = kvMap.get("driver_race_raw");
						}
						
						if (race.contains("w")) {
							values[i] = new IntWritable(1);
						} else if (race.contains("b")) {
							values[i + 1] = new IntWritable(1);
						} else if (race.contains("h")) {
							values[i + 2] = new IntWritable(1);
						} else if (race.contains("a")) {
							values[i + 3] = new IntWritable(1);
						} else {
							values[i + 4] = new IntWritable(1);
						}
						
						i += 5;
						
						/*
						 * 	values[44]	==	less than 15 years old
						 *  values[45]	==	15 yo
						 *  values[46]	==	16 yo
						 *  values[47]	==	17 yo
						 *  values[48]	==	18 yo
						 *  values[49]	==	19 yo
						 *  values[50]	==	20 yo
						 *  values[51]	==	21 yo
						 *  values[52]	==	22 yo
						 *  values[53]	==	23 yo
						 *  .
						 *  .
						 *  .
						 *  values[114] ==	84 years old
						 *  values[115] ==	85 years old
						 *  values[116] ==	more than 85 years old
						 *  values[117] ==	age unknown
						 */
						
						String age = "";
						
						if (kvMap.containsKey("driver_age")) {
							age = kvMap.get("driver_age");
						} else if (kvMap.containsKey("driver_age_raw")) {
							age = kvMap.get("driver_age_raw");
						}
						
						values[i + getAge(age)] = new IntWritable(1);
						i += 73;
						
						/*
						 *	values[118] ==	car registered in 1980
						 *	values[119] ==	car registered in 1981
						 *	.
						 *	.
						 *	.
						 *	values[149]	==	car registered in 2012
						 *	values[150]	==	car registered in 2013
						 *	values[151]	== 	car registered in 2014
						 *	values[153]	== 	registration year unknown
						 */
						
						String vehicle = "";
						
						if (kvMap.containsKey("vehicle_type")) {
							vehicle = kvMap.get("vehicle_type");
						}
						
						values[i + getVehicleYear(vehicle)] = new IntWritable(1);
						i += 37;
						
						
						
						/*
						 *	values[154]	==	Speeding
						 *	values[155]	==	DUI
						 *	values[156]	==	License
						 *	values[157]	==	Paperwork
						 *	values[158]	==	Stop Sign / Traffic Light Violation
						 *	values[159]	==	Safe Movement
						 *	values[160]	==	Unknown/Other
						 */
												
						String violation  = "";
						
						if (kvMap.containsKey("violation")) {
							violation = kvMap.get("violation");
						} else if (kvMap.containsKey("violation_raw")) {
							violation = kvMap.get("violation_raw");
						}
						
						if (violation.contains("speed")) {
							values[i] = new IntWritable(1);
						} else if (violation.contains("dui") || violation.contains("alcohol") || violation.contains("dwi")) {
							values[i + 1] = new IntWritable(1);
						} else if (violation.contains("license")) {
							values[i + 2] = new IntWritable(1);
						} else if (violation.contains("paper") || violation.contains("work")) {
							values[i + 3] = new IntWritable(1);
						} else if (violation.contains("sign") || violation.contains("stop") || violation.contains("traffic") || violation.contains("light")) {
							values[i + 4] = new IntWritable(1);
						} else if (violation.contains("safe") || violation.contains("movement")) {
							values[i + 5] = new IntWritable(1);
						} else {
							values[i + 6] = new IntWritable(1);
						}
						
						i += 7;
						
						/*
						 * values[161]	== search conducted
						 * values[162] 	== search not conducted
						 * values[163] 	== unknown if search conducted
						 */
						
						String searchConducted = "";
						
						if (kvMap.containsKey("search_conducted")) {
							searchConducted = kvMap.get("search_conducted");
						}
						
						if (searchConducted.contains("t") || searchConducted.contains("y")) {
							values[i] = new IntWritable(1);
						} else if (searchConducted.contains("f") || searchConducted.contains("n")) {
							values[i + 1] = new IntWritable(1);
						} else {
							values[i + 2] = new IntWritable(1);
						}
						
						i += 3;
						
						/*
						 * values[164] 	== consented search
						 * values[165] 	== k9
						 * values[166]	== frisk
						 * values[167]	== incident to arrest (warrantless search in times of arrest)
						 * values[168]	== other / unknown search type
						 */
						
						String searchType = "";
						
						if (kvMap.containsKey("search_type")) {
							searchType = kvMap.get("search_type");
						} else if (kvMap.containsKey("search_type_raw")) {
							searchType = kvMap.get("search_type_raw");
						}
						
						if (searchType.contains("consent")) {
							values[i] = new IntWritable(1);
						} else if (searchType.contains("k9") || searchType.contains("dog")) {
							values[i + 1] = new IntWritable(1);
						} else if (searchType.contains("frisk")) {
							values[i + 2] = new IntWritable(1);
						} else if (searchType.contains("arrest")) {
							values[i + 3] = new IntWritable(1);
						} else {
							values[i + 4] = new IntWritable(1);
						}
						
						i += 5;
						
						/*
						 * values[169]	== contraband was found
						 * values[170]	== contraband was not found
						 * values[171]	== unknown whether or not contraband was found
						 */
						
						String contrabandFound = "";
						
						if (kvMap.containsKey("contraband_found")) {
							contrabandFound = kvMap.get("contraband_found");
						}
						
						if (contrabandFound.contains("t") || contrabandFound.contains("y")) {
							values[i] = new IntWritable(1);
						} else if (contrabandFound.contains("f") || contrabandFound.contains("n")) {
							values[i + 1] = new IntWritable(1);
						} else {
							values[i + 2] = new IntWritable(1);
						}
						
						i += 3;
						
						/*
						 * values[172]	== verbal warning
						 * values[173]	== written warning
						 * values[174]	== arrest or citation
						 * values[175]	== unknown
						 */
						
						String stopOutcome = "";
						
						if (kvMap.containsKey("stop_outcome")) {
							stopOutcome = kvMap.get("stop_outcome");
						}
						
						if (stopOutcome.contains("verbal")) {
							values[i] = new IntWritable(1);
						} else if (stopOutcome.contains("written")) {
							values[i + 1] = new IntWritable(1);
						} else if (stopOutcome.contains("arrest") || stopOutcome.contains("citation")) {
							values[i + 2] = new IntWritable(1);
						} else {
							values[i + 3] = new IntWritable(1);
						}
						
						i += 4;
						
						/*
						 * values[176]	==	was arrested
						 * values[177]	==	was not arrested
						 * values[178]	==	unknown if arrested
						 */
						
						String isArrested = "";
						
						if (kvMap.containsKey("is_arrested")) {
							isArrested = kvMap.get("is_arrested");
						}
						
						if (isArrested.contains("t") || isArrested.contains("y")) {
							values[i] = new IntWritable(1);
						} else if (isArrested.contains("f") || isArrested.contains("n")) {
							values[i + 1] = new IntWritable(1);
						} else {
							values[i + 2] = new IntWritable(1);
						}
						
						i += 3;
						
						/*
						 * values[179]	==	vehicle was from a different state
						 * values[180]	==	vehicle was from same state
						 * values[181]	==	unknown which state the vehicle was from
						 */
						
						String outOfState = "";
						
						if (kvMap.containsKey("out_of_state")) {
							outOfState = kvMap.get("out_of_state");
						}
						
						if (outOfState.contains("t") || outOfState.contains("y")) {
							values[i] = new IntWritable(1);
						} else if (outOfState.contains("f") || outOfState.contains("n")) {
							values[i + 1] = new IntWritable(1);
						} else {
							values[i + 2] = new IntWritable(1);
						}
						
						i += 3;
						
						/*
						 * values[182]	==	stop is drugs related
						 * values[183]	==	stop was not drugs related
						 * values[184]	==	unknown whether the stop was drugs related
						 */
						
						String drugsRelatedStop = "";
						
						if (kvMap.containsKey("drugs_related_stop")) {
							drugsRelatedStop = kvMap.get("drugs_related_stop");
						}
						
						if (drugsRelatedStop.contains("t") || drugsRelatedStop.contains("y")) {
							values[i] = new IntWritable(1);
						} else if (drugsRelatedStop.contains("f") || drugsRelatedStop.contains("n")) {
							values[i + 1] = new IntWritable(1);
						} else {
							values[i + 2] = new IntWritable(1);
						}
						
						String location = "";
						
						if (kvMap.containsKey("county_name")) {
							location = kvMap.get("county_name");
						} else if (kvMap.containsKey("district")) {
							location = kvMap.get("district");
						}
						
						aw.set(values);
						context.write(new Text(config.get("state") + " " + location), aw);
					}
				}
			}
			
			

		}
	}
	
	
	private int getVehicleYear(String vehicle) {
		if (vehicle.trim().equals("")) return 36;
		
		Pattern yearPattern = Pattern.compile("\\d{4}");
		Matcher yearMatcher = yearPattern.matcher(vehicle);
		
		while (yearMatcher.find()) {
			int year = Integer.valueOf(yearMatcher.group(0));
			
			if (year >= 1980 && year <= 2014) {
				return year - 1980;
			}
		}
		
		return 36;
	}


	private int getAge(String a) {
		
		if (a.trim().equals("")) return 73;
		
		if (a.contains(".")) {
			a = a.substring(0, a.indexOf("."));
		}
		
		int age = 0;
		
		try {
			age = Integer.parseInt(a);
		} catch (NumberFormatException e) {
			DateFormat format = new SimpleDateFormat("yyyy-MM-dd");
			try {
				
				Calendar birthdate = Calendar.getInstance(Locale.US);
				birthdate.setTime(format.parse(a));
				
				Calendar today = Calendar.getInstance(Locale.US);
				today.setTime(new Date());
				
				age = today.get(Calendar.YEAR) - birthdate.get(Calendar.YEAR);
				if (birthdate.get(Calendar.MONTH) > today.get(Calendar.MONTH)) {
					age -= 1;
				} else if (birthdate.get(Calendar.MONTH) == today.get(Calendar.MONTH)) {
					if (birthdate.get(Calendar.DAY_OF_MONTH) > today.get(Calendar.DAY_OF_MONTH)) {
						age -= 1;
					}
				}
			} catch (ParseException e1) {
				return 73;
			}
		}
		
		if (age == 0) {
			return 72;
		} else if (age < 15) {
			return 0;
		} else if (age > 85) {
			return 71;
		} else {
			return age - 15;
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
