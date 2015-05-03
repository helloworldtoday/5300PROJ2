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
			job.setMapperClass(SimplePR.SimpleMap.class);
			job.setReducerClass(SimplePR.SimpleReduce.class);  
			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);
			
			String inputPath = i == 0 ? "input" : "stage" + i;
			String outputPath = "stage" + (i + 1);
		    
			FileInputFormat.addInputPath(job, new Path(inputPath));
			FileOutputFormat.setOutputPath(job, new Path(outputPath));       
			job.waitForCompletion(true);
			
			double residualAvg = job.getCounter().findCounter("Counter.RESIDUAL").getValue();
			double resAvg = (residualAvg / base) / numNode;
			
			DecimalFormat four = new DecimalFormat("#0.0000");
			System.out.println("Iteration " + i + "; " + "Residual " + four.format(resAvg));
        	
			job.getCounters().findCounter("Counter.RESIDUAL").setValue(0L);
		}
	}
	
	public static class SimpleMap extends Mapper<LongWritable, Text, Text, Text> {
		// TODO map function
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// format: source, oldPR, list of destinations(maybe no outgoing)
			String perLine = value.toString().trim();
	  	  	String[] substring = perLine.split("\\s+");
	  	  	
	  	  	Text node = new Text(substring[0]);
	  	  	double pageRankOld = Double.parseDouble(substring[1]);
	  	  	int degrees = 0;
	  	  	String outgoing = ""; 
	  	  	
	  	  	if (substring.length > 2) { // have outgoing
	  	  		String[] outgoings = substring[2].split(",");
	  	  		degrees = outgoings.length;
	  	  		outgoing = substring[2];
	  	  	}
	  	  	
	  	  	Text key = new Text(node);
	  	  	Text value1 = new Text("pageRank;" + String.valueOf(pageRankOld) + ";" + outgoing);
	  	  	context.write(key, value);
	  	  	
	  	  	if (outgoing != "") {
	  	  		double pageRankNew = Double.parseDouble(substring[1]);
	  	  		Text value2 = new Text(String.valueOf(pageRankNew)); 
	  	  		String[] outgoingAll = outgoing.split(",");
	  	  		
				for (String item: outgoingAll) {
					key = new Text(item);
					context.write(key, value);
				}
	  	  	}			
		}
	}
	
	public static class SimpleReduce extends Reducer<Text, Text, Text, Text> {
		// TODO reduce function
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			Text list = new Text();
			double damping = 0.85;
			double pageRankAll = 0.0;
			double pageRankNew = 0.0;
			double pageRankOld = 0.0;
			double residual = 0.0;
			double randomJump = 1 - damping;
			
			String outgoing = "";
			String output = "";
			
			for (Text val : values) {
				String[] array = list.toString().split(";");
				if (array[0].equals("pageRank")) {
					pageRankOld = Double.parseDouble(array[1]);
					if (array.length > 2) {
						outgoing = array[2];
					} 
				} else {
					double pageRankPer = new Double(Double.parseDouble(array[0]));
					pageRankAll += pageRankPer;
				}
			} 
			pageRankNew = randomJump / numNode + (damping * pageRankAll);
			residual = Math.abs(pageRankOld - pageRankNew) / pageRankNew;
			long residualPer = (long) Math.ceil(residual * base);
			context.getCounter("Counter.RESIDUALS").increment(residualPer);
			
			Text value = new Text(output = pageRankNew + " " + " " + outgoing);
			context.write(key, value);
		}
	}
}
