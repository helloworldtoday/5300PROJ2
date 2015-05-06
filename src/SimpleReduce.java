import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.io.Text;

public class SimpleReduce extends Reducer<Text, Text, Text, Text> {
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			
		double damping = 0.85;
		double pageRankAll = 0.0;
		double pageRankNew = 0.0;
		double pageRankOld = 0.0;
		double residual = 0.0;
		double randomJump = 1 - damping;
			
		String outgoing = "";
		String output = "";
		
		// split outgoing list
		// read old PageRank & outgoing list
		// or calculate new PageRank (unit)	
		for (Text val : values) {
			String[] array = val.toString().split(";");
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
		// sum new PageRank for each node, calculate residual for each node
		pageRankNew = randomJump / SimplePR.numNode + (damping * pageRankAll);
		residual = Math.abs(pageRankOld - pageRankNew) / pageRankNew;
		long residualPer = (long) Math.ceil(residual * SimplePR.base);
		context.getCounter(Counter.RESIDUAL).increment(residualPer);
		
		// format: key: node; value: new_pageRank outgoing_list	
		Text value = new Text(pageRankNew + " " + outgoing);
		context.write(key, value);
	}
}
