# CS5300 Project 2 Spring 2015
Fast Convergence PageRank in Hadoop  
http://edu-cornell-cs-cs5300s15-proj2.s3.amazonaws.com/project2.html

**Yu-Che Hsueh & Shaoke Xu**

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
	double rejectMin = 0.99 * fromNetID; (0.7623)  
	double rejectLimit = rejectMin + 0.01; (0.7724)  

Number of Nodes: **678,386** from *685,230*  

## Code Structure
### Simple Map Reduce

### Blocked Map Reduce
#### PageRankBlock.java
#### PageRankBlockMapper.java
#### PageRankBlockReducer.java
#### NodeData.java

## Reference
AWS Documentation  
https://github.com/MatthewGreen/CS5300-Project2  