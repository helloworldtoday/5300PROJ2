import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;

public class PageRankBlockMapper extends Mapper<LongWritable, Text, Text, Text> {

	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		// Format: block node pageRank <edges>
		String line = value.toString();
		line = line.trim();
		String[] temp = line.split("\\s+");

		Integer blockID = new Integer(temp[0]);
		Integer node = new Integer(temp[1]);
		Float pageRank = new Float(temp[2]);
		
		// Check for outgoing edge
		String edgeList = "";
		if (temp.length == 4) {
			edgeList = temp[3];
		}

		// Format: <blockID | "PR nodeID pageRank <outgoing edgelist>" >
		Text mapperKey = new Text(blockID.toString());
		Text mapperValue = new Text("PR " + node.toString() + " " + String.valueOf(pageRank) + " " + edgeList);
		context.write(mapperKey, mapperValue);

		// For each u -> v
		// Decide whether v is in block b(u), if not we emit block edge (BE) else emit boundary condition (BC)
		if (edgeList != "") {
			String[] edgeListArray = edgeList.split(",");

			for (int i = 0; i < edgeListArray.length; i++) {
				
				Integer blockIDOut = new Integer(lookupBlockID(Integer.parseInt(edgeListArray[i])));
				mapperKey = new Text(blockIDOut.toString());
				
				// Node inside same block
				if (blockIDOut.equals(blockID)) {
					// Format: "BE u v"
					mapperValue = new Text("BE " + node.toString() + " " + edgeListArray[i]);
				
				// Node outside of block but point into block
				} else {
					// Since PR_0(u) / deg(u) won't change during iterations so just pass it to reducer
					Float pageRankFactor = new Float(pageRank / edgeListArray.length);
					String pageRankFactorString = String.valueOf(pageRankFactor);
					
					// Format: "BC u v partial_PR"
					mapperValue = new Text("BC " + node.toString() + " " + edgeListArray[i] + " " + pageRankFactorString);
				}
				context.write(mapperKey, mapperValue);
			}
		}
	}

	// lookup the block ID in a hardcoded list based on the node ID
	public static int lookupBlockID(int nodeID) {
		
		// Return hash result
		return (3 * nodeID + 6) % 68;
	}
}
