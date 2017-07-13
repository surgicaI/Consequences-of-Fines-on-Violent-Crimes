import java.io.*;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TaxesFinesSortedMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws
        IOException, InterruptedException {

            String delim = "[,]+";
            String[] line = value.toString().split(delim);
            String city_or_county = line[3];
            if (city_or_county.equals("city")) {
                context.write(new Text(city_or_county), new Text(line[0]+
                            ","+line[1]+","+line[2]+","+line[4]+","+
                            line[5]+","+line[6]+","+line[7]+","+line[8]));
            } else {
                context.write(new Text(city_or_county), new Text(line[0]+
                            ","+line[1]+","+line[2]+","+line[4]+","+
                            line[5]+","+line[6]+","+line[7]+","+line[8]));
            }
        } /* ~map */
} /* ~Mapper */

/* 2013 Fine and Tax Data Schema (9 fields)
 * 1. place:               String
 * 2. state:               String
 * 3. population:          Int
 * 4. local_govt_type:     String ("city" || "county)
 * 5. fines_forfeits:      Int
 * 6. total_tax:           Int
 * 7. fines_to_tax:        Double
 * 8. tax_per_capita:      Double
 * 9. fines_per_capita:    Double
 *
 * Source: Sunlight Foundation
 */
