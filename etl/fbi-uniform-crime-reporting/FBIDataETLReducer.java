import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.lang.StringBuilder;

public class FBIDataETLReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String separater = ",";
        String state = "";
        String agency = "";
        String county = "";
        String data = "";
        int num_values = 0;
        String key_string = key.toString();
        StringBuilder resultBuilder = new StringBuilder();
        resultBuilder.append(separater);
        for (Text value : values) {
            String value_string = value.toString();
            num_values++;
            if(state.isEmpty()) {
                if(key_string.length() > 3) {
                    state = key_string.substring(0,2);
                    agency = key_string.substring(3).replace("-", " ");
                }else{
                    // Error
                    // context.write(new Text("ZZZZ-Error"), key);
                    return;
                }
            }
            if(value_string.startsWith("county:")) {
                    county = value_string.substring(7);
            }else if(value_string.startsWith("data:")) {
                    data = value_string.substring(5);
            }
        }
        if(num_values == 2 && !data.isEmpty()) {
            resultBuilder.append(county).append(separater);
            // resultBuilder.append(agency).append(separater);
            resultBuilder.append(data);
            context.write(new Text(state), new Text(resultBuilder.toString()));
        }else if(num_values > 2 && !data.isEmpty()) {
            // Error
            // context.write(new Text("ZZZZ-Error"), key);
        }
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        context.write(new Text("state"), new Text(",County, Agency, Months Reporting,Population Coverage,Violent crime total,Murder and nonnegligent manslaughter,Legacy rape1,Revised rape2,Robbery,Aggravated assault,Property crime total,Burglary,Larceny-theft,Motor vehicle theft,Violent Crime rate,Murder and nonnegligent manslaughter rate,Legacy rape rate1,Revised rape rate2,Robbery rate,Aggravated assault rate,Property crime rate,Burglary rate,Larceny-theft rate,Motor vehicle theft rate"));
    }
}
