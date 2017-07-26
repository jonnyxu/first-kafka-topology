# first-kafka-topology
My first topology project for studying Apache Storm and Kafka

1. Running Topology on local cluster:
	 mvn compile exec:java -Dexec.classpathScope=compile -Dexec.mainClass=com.jonny.storm.FirstKafkaTopology

2. Deploying Topology to Storm cluster:
   ./storm/bin/storm jar first-kafka-topology-1.0-SNAPSHOT-jar-with-dependencies.jar com.jonny.storm.FirstKafkaTopology FirstKafkaTopology

3. Stopping Storm Topology:
   storm kill FirstKafkaTopology
