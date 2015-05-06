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
	static int iteration = 5;
	static int numNode = 685230;
	static int base = 1000000;
	
	public static void main(String[] args) throws Exception {
		
		for (int i = 0; i < iteration; i++) {
			Configuration conf = new Configuration();
			Job job = new Job(conf, "pageRank_" + i);
			
			// job setting
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);      
			job.setMapperClass(SimpleMap.class);
			job.setReducerClass(SimpleReduce.class);  
			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);
			job.setJarByClass(SimplePR.class);
			
			// inputPath is outputPath in last iteration (from second iteration)
			String inputPath = i == 0 ? "input" : "stage" + (i - 1);
			String outputPath = "stage" + i;
		    
			FileInputFormat.addInputPath(job, new Path(inputPath));
			FileOutputFormat.setOutputPath(job, new Path(outputPath));       
			job.waitForCompletion(true);
			
			// calculate average residual and store it in Counter
			double residualAvg = job.getCounters().findCounter(Counter.RESIDUAL).getValue();
			double resAvg = (residualAvg / base) / numNode;
			
			// keep six decimal digits
			DecimalFormat six = new DecimalFormat("#0.000000");
			System.out.println("Iteration " + i + "; " + "Residual " + six.format(resAvg));
        	
        		// reset residual
			job.getCounters().findCounter(Counter.RESIDUAL).setValue(0L);
		}
	}	
}

