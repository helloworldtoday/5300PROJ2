# CS5300 Project 2 Spring 2015
Fast Convergence PageRank in Hadoop  
http://edu-cornell-cs-cs5300s15-proj2.s3.amazonaws.com/project2.html

**Yu-Che Hsueh & Shaoke Xu**

## Folders
1. input: stores pre-processed input data.  
2. result: contains files recorded the residual error, average number of iteration per block and page rank value of the two lowest-numbered nodes in each block.  
3. src: source codes  
4. jar: backup jar file

## Input data format
* Simple PageRank: <nodeID> <initial_PageRank> <outgoing_list>  
* Block PageRank: <blockID> <nodeID> <initial_PageRank> <outgoing_list>

## Data processing

1. WebPageScanner.java: capture data from web page
2. RemoveReject.java: filter data by function in instruction
3. CombineOutgoing.java: combine outgoing nodes for each source
4. addBlockID.java: add block ID in the first column
5. RandomPartition.java: use hash function to add blockID (for 7.2 Random Block Partition)

## Simple PageRank

1. SimplePR.java: main function. Setting parameters of job and input/output path and getting average residual from Counter.
2. SimpleMap.java: mapper function. Calculating partial new PageRank for each node.
3. SimpleReduce.java: reducer function. Calculating total new PageRank for each node and updating residual by Counter.
4. Counter.java: enum class that store residual error

## Block PageRank
1. NodeData.java: self-defined data structure used to represent a node, including nodeID, edgeList, pageRank and degrees.
2. PageRankBlock.java: main function. Setting parameters of job and input/output path and getting average residual from Counter. Iterating loop until satisfy the requirement of residual error.
3. PageRankBlockMapper.java: mapper function. For more information, see slides.
4. PageRankBlockReducer.java: reducer function. For more information, see slides.

## How To Run Block Map Reduce on EMR

1. Compile JAR by Eclipse  
Remember to use Java 1.7 and include hadoop external JAR

2. Upload data to AWS S3

3. Configure Command Line Interface(CLI) and create key pair  
`http://docs.aws.amazon.com/cli/latest/userguide/tutorial-ec2-ubuntu.html`

4. Create EMR cluster by console  

5. SSH to master  
`ssh hadoop@ec2-52-11-159-61.us-west-2.compute.amazonaws.com -i ~/devenv-key.pem`

6. Upload JAR  
`scp -i ~/devenv-key.pem PageRankBlock.jar hadoop@ec2-52-11-159-61.us-west-2.compute.amazonaws.com:PageRankBlock.jar`

7. Run JAR  
`bin/hadoop jar PageRankBlock.jar PageRankBlock s3://<bucket_name>/<file_name> s3://<output_bucket>/`

## Filter Parameters

Compute filter parameters for netID sx88  
	double fromNetID = 0.77;  
	double rejectMin = 0.9 * fromNetID; (0.693)  
	double rejectLimit = rejectMin + 0.01; (0.703)  

Number of Nodes: **619,588** from *685,230*  

## Comparison:
1. Jacobi vs Gauss-Seidel  
From the results (appendix), we can notice that the residual errors in two methods are similar while the average numbers of iterations per block are different, especially at the beginning. Gauss method performs better since it reduces the times of iteration. For example, average numbers of iterations per block in Gauss are 9.794118, 5.352941, 4.529412, etc. There are 16.029411, 7.897059, 5.9558825 and so on in the Jacobi method.

2. Convergence of node-by-node, intelligently blocked and randomly blocked
  1. node-by-node: 21 times
  2. intelligently blocked: 6 times
  3. randomly blocked(Jacobi): 22 times
  4. randomly blocked(Gauss): 22 times

## Reference
AWS Documentation  
https://github.com/MatthewGreen/CS5300-Project2  