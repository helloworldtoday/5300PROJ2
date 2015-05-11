import java.io.IOException;
import java.text.DecimalFormat;
import java.util.*;      

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.io.Text;


public class SimplePR {
	static int iteration = 23;
	static int numNode = 685230;
	static int base = 1000000;
	
	public static void main(String[] args) throws Exception {
			
		for (int i = 0; i < iteration; i++) {
			Configuration conf = new Configuration();
			Job job = new Job(conf, "pageRank_" + i);
			// job setting
			job.setJarByClass(SimplePR.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);      
			job.setMapperClass(SimpleMap.class);
			job.setReducerClass(SimpleReduce.class);  
			//job.setInputFormatClass(TextInputFormat.class);
			//job.setOutputFormatClass(TextOutputFormat.class);
			
			
			String inputPath = (i == 0) ? args[0] : (args[1] + "/stage" + (i - 1));
			String outputPath = args[1] + "/stage" + i;
		    
			FileInputFormat.addInputPath(job, new Path(inputPath));
			FileOutputFormat.setOutputPath(job, new Path(outputPath));       
			job.waitForCompletion(true);
			
			double residualAvg = job.getCounters().findCounter(Counter.RESIDUAL).getValue();
			double resAvg = (residualAvg / base) / numNode;
			
			DecimalFormat six = new DecimalFormat("#0.000000");
			System.out.println("Iteration " + i + "; " + "Residual " + six.format(resAvg));
        	
			job.getCounters().findCounter(Counter.RESIDUAL).setValue(0L);
		}
	}	
}

