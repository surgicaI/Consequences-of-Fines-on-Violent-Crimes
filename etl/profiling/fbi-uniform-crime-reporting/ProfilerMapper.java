import java.io.IOException;
import java.lang.StringBuilder;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ProfilerMapper extends Mapper<LongWritable, Text, Text, Text> {
private static final int FBI_DATA_FIELDS = 24;
private static final int ICPSR_DATA_FIELDS = 51;
private static final String[] match_tokens = {"police", "county", "sherrif", "department", "dept", "city"};

@Override
public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] fields = line.split(",",-1);
        String new_key = "", new_value = "";
        if(fields.length == FBI_DATA_FIELDS){
            new_value = "";
            String agency = fields[0];
            String state = fields[1].toUpperCase();
            if(state.equalsIgnoreCase("state")){
                return;
            }
            String[] agency_fields = agency.toLowerCase().split("\\s+");
            new_key = state;
            int index = 0;
            outerloop:
            for(String agency_field: agency_fields){
                index++;
                for(String match_token: match_tokens){
                    if(agency_field.equalsIgnoreCase(match_token) || index > 2){
                        break outerloop;
                    }else{
                        new_key = new_key + "-" + agency_field;
                    }
                }
            }
            StringBuilder sbStr = new StringBuilder();
            sbStr.append("data:");
            for(int i=2; i<fields.length; i++){
                if (i > 2)
                    sbStr.append(",");
                sbStr.append(fields[i]);
            }
            new_value = sbStr.toString();
        }else if(fields.length == ICPSR_DATA_FIELDS){
            new_value = "county:";
            String state = fields[0].toUpperCase();
            String county = fields[1].toLowerCase();
            String agency = fields[2].toLowerCase();
            if(state.equalsIgnoreCase("state")){
                return;
            }
            String[] agency_fields = agency.toLowerCase().split("\\s+");
            new_key = state;
            int index = 0;
            outerloop:
            for(String agency_field: agency_fields){
                index++;
                for(String match_token: match_tokens){
                    if(agency_field.equalsIgnoreCase(match_token) || index > 2){
                        break outerloop;
                    }else{
                        new_key = new_key + "-" + agency_field;
                    }
                }
            }
            new_value += county;
        }else{
            // Some error in code or data
            return;
        }
        context.write(new Text(new_key), new Text(new_value));
    }
}
