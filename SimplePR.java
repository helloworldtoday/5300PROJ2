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
import org.w3c.dom.Text;


public class SimplePR {
	static int iteration = 10;
	static int numNode = 685228;
	static int base = 10000;
	
	public static void main(String[] args) throws Exception {
		// TODO main function		
		for (int i = 0; i < iteration; i++) {
			Configuration conf = new Configuration();
			Job job = new Job(conf, "pageRank_" + i);
			// job setting
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(LongWritable.class);      
			job.setMapperClass(SimpleMap.class);
			job.setReducerClass(SimpleReduce.class);  
			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);
			
			String inputPath = i == 0 ? "input" : "stage" + i;
			String outputPath = "stage" + (i + 1);
		    
			FileInputFormat.addInputPath(job, new Path(inputPath));
			FileOutputFormat.setOutputPath(job, new Path(outputPath));       
			job.waitForCompletion(true);
			
			double residualAvg = job.getCounters().findCounter(Counter.RESIDUAL).getValue();
			double resAvg = (residualAvg / base) / numNode;
			
			DecimalFormat four = new DecimalFormat("#0.0000");
			System.out.println("Iteration " + i + "; " + "Residual " + four.format(resAvg));
        	
			job.getCounters().findCounter(Counter.RESIDUAL).setValue(0L);
		}
	}	
}

