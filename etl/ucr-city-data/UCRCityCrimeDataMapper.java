import java.lang.Exception;
import java.io.IOException;
import java.lang.StringBuilder;
import org.apache.commons.csv.*;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class UCRCityCrimeDataMapper extends Mapper<LongWritable, Text, Text, Text> {
private static final int FINES_DATA_FIELDS = 9;
private static final String[] match_tokens = {" and ", "county", "borough", "town", "village", "municipality"};

@Override
public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        CSVParser parser;
        CSVRecord record;
        try{
            parser = CSVParser.parse(line, CSVFormat.EXCEL);
            Iterator<CSVRecord> iterator = parser.iterator();
            if(iterator.hasNext())
                record = iterator.next();
            else{
                //No such element exception
                return;
            }
        }catch(IOException e){
            return;
        }
        String new_key = "", new_value = "";
        if(record.size() <= FINES_DATA_FIELDS){
            new_value = "";
            String place = record.get(0);
            String state = record.get(1).toUpperCase();
            if(state.equalsIgnoreCase("state")){
                return;
            }
            new_key = state;
            for(String match_token: match_tokens){
                place = place.replace(match_token,"");
            }
            new_key = new_key + "," + place;
            StringBuilder sbStr = new StringBuilder();
            sbStr.append("fines:");
            for(int i=2; i<record.size(); i++){
                sbStr.append(",").append(record.get(i).replace(",",""));
            }
            new_value = sbStr.toString();
        }else{
            String state = record.get(0).toUpperCase();
            String place = record.get(1).toLowerCase();

            if(state.equalsIgnoreCase("state")){
                return;
            }
            new_key = state + "," + place;

            StringBuilder sbStr = new StringBuilder();
            sbStr.append("crime:");
            for(int i=2; i<record.size(); i++){
                sbStr.append(",").append(record.get(i).replace(",",""));
            }
            new_value = sbStr.toString();
        }
        context.write(new Text(new_key), new Text(new_value));
    }
}
