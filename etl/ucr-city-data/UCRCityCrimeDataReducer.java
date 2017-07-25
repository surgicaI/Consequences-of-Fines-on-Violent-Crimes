import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.lang.StringBuilder;

public class UCRCityCrimeDataReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String data_fine = "", data_crime = "";
        int num_values = 0;
        String new_value = "";

        for (Text value : values) {
            String value_string = value.toString();
            num_values++;

            if(value_string.startsWith("crime:")) {
                data_crime = value_string.replace("crime:","");
            }else if(value_string.startsWith("fine:")) {
                data_fine = value_string.replace("fine:","");
            }
        }
        if(num_values == 2 && !data_crime.isEmpty() && !data_fine.isEmpty()) {
            new_value += data_crime;
            new_value += data_fine;
            context.write(key, new Text(new_value));
        }else if(num_values > 2 && !data_crime.isEmpty() && !data_fine.isEmpty()) {
            // Error
            // context.write(new Text("ZZZZ-Error"), key);
        }
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        context.write(new Text("State"), new Text(",City,Population,Violent crime,Murder and nonnegligent manslaughter,Rape (revised definition)1,Rape (legacy definition)2,Robbery,Aggravated assault,Property crime,Burglary,Larceny-theft,Motor vehicle theft,population,fines_forfeits,total_tax,fines_to_tax,fines_per_capita,tax_per_capita"));
    }
}
