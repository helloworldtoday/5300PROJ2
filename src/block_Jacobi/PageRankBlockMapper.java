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
		
		int partitionSize = 10000;
		int[] blockBoundaries = { 
				 0,  10328,  20373,  30629,  40645,  50462,  60841,  70591,  80118,  90497,
			100501, 110567, 120945, 130999, 140574, 150953, 161332, 171154, 181514, 191625,
			202004, 212383, 222762, 232593, 242878, 252938, 263149, 273210, 283473, 293255,
			303043, 313370, 323522, 333883, 343663, 353645, 363929, 374236, 384554, 394929,
			404712, 414617, 424747, 434707, 444489, 454285, 464398, 474196, 484050, 493968,
			503752, 514131, 524510, 534709, 545088, 555467, 565846, 576225, 586604, 596585,
			606367, 616148, 626448, 636240, 646022, 655804, 665666, 675448, 685230
		};

		int blockID = (int) Math.floor(nodeID / partitionSize);
		int testBoundary = blockBoundaries[blockID];
		if (nodeID < testBoundary) {
			blockID--;
		}
		
		return blockID;
	}
}
