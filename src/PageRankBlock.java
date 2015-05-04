import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


// blocked version of pageRank calculations using MapReduce
public class PageRankBlock {
	
	// use a hadoop counter to track the total residual error so we can compute the average at the end
	public static enum MRCounters {
	    RESIDUAL_ERROR
	};
	public static final int totalNodes = 685230;        // total # of nodes in the input set
	public static final int totalBlocks = 68;           // total # of blocks
	public static final int precision = 1000000;        // this allows us to store the residual error value in the counter as a long
	private static final Float thresholdError = 0.001f;	// the threshold to determine whether or not we have converged
    
	public static void main(String[] args) throws Exception {
	
		int i = 0;  // stage counter
		Float residualErrorAvg = 0.0f;
		
		// iterate to convergence 
        do {
            Job job = new Job();
            
            // Set a unique job name
            job.setJobName("block_"+ (i+1));
            job.setJarByClass(PageRankBlock.class);

            // Set output key and value type
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            // Set Mapper and Reducer class
            job.setMapperClass(PageRankBlockMapper.class);
            job.setReducerClass(PageRankBlockReducer.class);
            
            // Set input and output path
            // use user input file in the first iteration
            String inputPath = (i == 0)? args[0] : (args[1] + "/stage" + i);
            String outputPath = args[1] + "/stage" + (i+1);
                        
            FileInputFormat.addInputPath(job, new Path(inputPath)); 	
            FileOutputFormat.setOutputPath(job, new Path(outputPath));
            
            // execute the job and wait for completion
            job.waitForCompletion(true);
            
            // Compute average residual error
            residualErrorAvg = (float) job.getCounters().findCounter(MRCounters.RESIDUAL_ERROR).getValue() / precision / totalBlocks;
            String residualErrorString = String.format("%.4f", residualErrorAvg);
            System.out.println("Residual error for iteration " + i + ": " + residualErrorString);
            
            // reset Hadoop counter
            job.getCounters().findCounter(MRCounters.RESIDUAL_ERROR).setValue(0L);
            i++;
            
        } while (residualErrorAvg > thresholdError);
    }
}

