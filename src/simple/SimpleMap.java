import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.io.Text;


	public class SimpleMap extends Mapper<LongWritable, Text, Text, Text> {
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
	  	  	
	  	  	Text mykey = new Text(node);
	  	  	Text value1 = new Text("pageRank;" + String.valueOf(pageRankOld) + ";" + outgoing);
	  	  	context.write(mykey, value1);
			// System.out.println("key: " + mykey);
			// System.out.println("value: " + value1);
	  	  	
	  	  	if (outgoing != "") {
	  	  		double pageRankNew = Double.parseDouble(substring[1]) / degrees;
	  	  		Text value2 = new Text(String.valueOf(pageRankNew)); 
	  	  		String[] outgoingAll = outgoing.split(",");
	  	  		
				for (String item: outgoingAll) {
					mykey = new Text(item);
					context.write(mykey, value2);
					// System.out.println("key: " + mykey);
					// System.out.println("value: " + value1);
				}
	  	  	}			
		}
	}
