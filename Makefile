hadoop = hadoop
HADOOP_PREFIX = /usr/local/Cellar/hadoop/2.6.0/libexec

bases = NodeData PageRankBlock PageRankBlockMapper PageRankReducer 
classDir = classes
sourceDir = src
javaFiles = src/*.java
classFiles = classes/*.class

CLASS_PATH = $(HADOOP_PREFIX)/share/hadoop/common/*:$(HADOOP_PREFIX)/share/hadoop/yarn/lib/*:$(HADOOP_PREFIX)/share/hadoop/mapreduce/lib/*:$(HADOOP_PREFIX)/share/hadoop/mapreduce/*:./
j = -classpath $(CLASS_PATH) #Xlint:deprecation


default : $(javaFiles)
	mkdir $(classDir); javac $j -d $(classDir) $(javaFiles); jar cvf PageRankBlock.jar $(classDir);echo $(classFiles); $(hadoop) jar PageRankBlock.jar PageRankBlock input output

clean : 
	rm -r $(classDir); rm -r output; rm PageRankBlock.jar;



