import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TaxesFinesSortedMR {

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: TaxesFinesSortedMR <input path> <output path>");
            System.exit(-1);
        }
        // Create an object to represent a Job
        Job job = new Job();

        // Tell Hadoop where to locate code when job shipped across cluster
        job.setJarByClass(TaxesFinesSortedMR.class);
        job.setJobName("TaxesFinesSorted MapReduce ETL");

        // Specify input and output locations to use for this job
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Set the mapper to use
        job.setMapperClass(TaxesFinesSortedMapper.class);
        job.setReducerClass(TaxesFinesSortedReducer.class);

        // Specify the type of the output
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Submit job and wait for it to finish
        // The argument specifies whether to print progress info to output
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    } /* ~main */
} /* ~Driver */
