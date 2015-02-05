import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class LogMapper extends Mapper<LongWritable, Text, Text, IntWritable> {


  @Override
  public void map(LongWritable key, Text value, Context context) 
	throws IOException, InterruptedException {
		
		String line = value.toString();
		
		
		if (line.contains("ERROR")) {
			
				context.write(new Text("ERROR"), new IntWritable(1));
			
			} else {
				
				if (line.contains("INFO")) {
					
					context.write(new Text("INFO"), new IntWritable(1));
				} else {
					
					if (line.contains("WARN")) {
						
						context.write(new Text("WARN"), new IntWritable(1));
					} else {
						
						if (line.contains("DEBUG")) {
							
							context.write(new Text("DEBUG"), new IntWritable(1));
						} else {
							
							if (line.contains("FATAL")) {
								
								context.write(new Text("FATAL"), new IntWritable(1));
							} else {
								
								if (line.contains("TRACE")) {
									
									context.write(new Text("TRACE"), new IntWritable(1));
								}
				
							  }
						}	
					}
				}
			
			} 			
  }
}	
