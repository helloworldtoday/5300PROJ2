import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;

public class PageRankBlockReducer extends Reducer<Text, Text, Text, Text> {

	private HashMap<String, Float> pageRanks = new HashMap<String, Float>(); // PR[v]
	private HashMap<String, ArrayList<String>> blockEdges = new HashMap<String, ArrayList<String>>(); // BE
	private HashMap<String, Float> boundaryConditions = new HashMap<String, Float>(); // BC
	
	// Store node data and pageRank for residual error calculation emit tuples
	private HashMap<String, NodeData> nodeDataMap = new HashMap<String, NodeData>();
	
	// Alpha parameter
	private Float dampingFactor = (float) 0.85;
	private Float randomJumpFactor = (1 - dampingFactor) / PageRankBlock.totalNodes;
	
	// Break condition
	private int maxIterations = 20;
	private Float threshold = 0.001f;
	
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		
		Text input = new Text();
		String[] inputTokens = null;
		
		Float residualError = (float) 0.0;
		
		ArrayList<String> temp = new ArrayList<String>();
		float tempBC = 0.0f;
		
		// Reset class variables
		pageRanks.clear();
		blockEdges.clear();
		boundaryConditions.clear();
		nodeDataMap.clear();
		
		Iterator<Text> itr = values.iterator();
		while (itr.hasNext()) {
			input = itr.next();
			inputTokens = input.toString().split(" ");
			// PR, nodes in block
			if (inputTokens[0].equals("PR")) {
				// Format: "PR nodeID pageRank <outgoing edgelist>"
				// Put into PR[v]
				String nodeID = inputTokens[1];
				Float pageRankOld = Float.parseFloat(inputTokens[2]);
				pageRanks.put(nodeID, pageRankOld);
				
				// Store node data
				NodeData node = new NodeData();
				node.setNodeID(nodeID);
				node.setPageRank(pageRankOld);
				if (inputTokens.length == 4) {
					// Outgoing node exist
					node.setEdgeList(inputTokens[3]);
					node.setDegrees(inputTokens[3].split(",").length);
				}
				nodeDataMap.put(nodeID, node);
				
			// BE, edges within block
			} else if (inputTokens[0].equals("BE")) {			
				// Format: "BE u v"
				if (blockEdges.containsKey(inputTokens[2])) {
					temp = blockEdges.get(inputTokens[2]);
				} else {
					//Initialize BE for this v
					temp = new ArrayList<String>();
				}
				temp.add(inputTokens[1]);
				blockEdges.put(inputTokens[2], temp);
				
			// BC, edges point into block
			} else if (inputTokens[0].equals("BC")) {
				// Format: "BC u v partial_PR"
				if (boundaryConditions.containsKey(inputTokens[2])) {
					tempBC = boundaryConditions.get(inputTokens[2]);
				} else {
					// Initialize BC for this v
					tempBC = 0.0f;
				}
				tempBC += Float.parseFloat(inputTokens[3]);

				// Doesn't have to store info of u, because page rank is already calculated
				boundaryConditions.put(inputTokens[2], tempBC);
			}		
		}
		
		int i = 0;
		do {
			i++;
			residualError = IterateBlockOnce();
		} while (i < maxIterations && residualError > threshold);
		context.getCounter(PageRankBlock.MRCounters.TOTAL_INNER_ITERATION).increment(i);
		System.out.println("\nNumber of inner iteration = " + i);
				
		// Compute residual error of each node
		residualError = 0.0f;

		for (String v : nodeDataMap.keySet()) {
			NodeData node = nodeDataMap.get(v);
			residualError += Math.abs(node.getPageRank() - pageRanks.get(v)) / pageRanks.get(v);
		}
		residualError = residualError / nodeDataMap.size();
		
		// Add the residual error to the counter
		long residualAsLong = (long) Math.floor(residualError * PageRankBlock.precision);
		context.getCounter(PageRankBlock.MRCounters.RESIDUAL_ERROR).increment(residualAsLong);
		
		//Format: < block | nodeID, pageRank <edges> >
		for (String v : nodeDataMap.keySet()) {
			
			NodeData node = nodeDataMap.get(v);
			String output = v + " " + pageRanks.get(v) + " " + node.getEdgeList();
			Text outputText = new Text(output);
			context.write(key, outputText);
		}
		cleanup(context);
	}
	
	// One function call equals to one iteration of block map reduce
	protected float IterateBlockOnce() {

		// resErr = the avg residual error for this iteration
		float resErr = 0.0f;
		
		// Iterate through all node in this block
		for (String v : nodeDataMap.keySet()) {
			float currPR = 0.0f;
			float prevPR = pageRanks.get(v);

			// Calculate pageRank using PR data from any BE nodes for this node
			if (blockEdges.containsKey(v)) {
				for (String u : blockEdges.get(v)) {
					NodeData uNode = nodeDataMap.get(u);
					currPR += (pageRanks.get(u) / uNode.getDegrees());
				}
			}
			
			// Add on any PR from nodes outside the block (BC)
			if (boundaryConditions.containsKey(v)) {
				currPR += boundaryConditions.get(v);
			}
	
	        // PR' = d * PR + (1-d) / N;
			currPR = (dampingFactor * currPR) + randomJumpFactor;
			// Update the global pageRank map
			pageRanks.put(v, currPR);
			// Track the sum of the residual errors
			resErr += Math.abs(prevPR - currPR) / currPR;
		}
		// Calculate the average residual error and return it
		resErr = resErr / nodeDataMap.size();
		return resErr;
	}

}

