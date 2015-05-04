# 5300PROJ2
Fast Convergence PageRank in Hadoop

## How To Run Block Map Reduce on EMR
1. Compile JAR by Eclipse
Remember to use Java 1.7 and include hadoop external JAR

2. Upload data to AWS S3

3. Configure Command Line Interface(CLI) and create key pair
`http://docs.aws.amazon.com/cli/latest/userguide/tutorial-ec2-ubuntu.html`

4. Create EMR cluster by console 

5. SSH to master
`ssh hadoop@ec2-52-11-159-61.us-west-2.compute.amazonaws.com -i ~/devenv-key.pem`

6. Upload jar
`scp -i ~/devenv-key.pem PageRankBlock.jar hadoop@ec2-52-11-159-61.us-west-2.compute.amazonaws.com:PageRankBlock.jar`

7. Run JAR
`bin/hadoop jar PageRankBlock.jar PageRankBlock s3://<bucket_name>/<file_name> s3://<output_bucket>/`