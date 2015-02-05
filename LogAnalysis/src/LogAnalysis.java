import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;

public class LogAnalysis {

  public static void main(String[] args) throws Exception {

    if (args.length != 2) {
      System.out.printf(
          "Usage: LogAnalysis <input dir> <output dir>\n");
      System.exit(-1);
    }

    @SuppressWarnings("deprecation")
	Job job = new Job();
    job.setJarByClass(LogAnalysis.class);
    job.setJobName("Log Analysis");
    FileInputFormat.setInputPaths(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    job.setMapperClass(LogMapper.class);
    job.setReducerClass(LogReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    boolean success = job.waitForCompletion(true);
    System.exit(success ? 0 : 1);
  }
}
