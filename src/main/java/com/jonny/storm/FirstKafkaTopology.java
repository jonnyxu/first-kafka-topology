package com.jonny.storm;

import kafka.api.OffsetRequest;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.spout.MultiScheme;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;

import com.jonny.storm.bolt.KafkaMessagingBolt;

/**
 * <p>Title: FirstKafkaTopology</p>
 * <p>Description: A Storm Topology that reads data from Apache Kafka topic - Messaging.</p>
 * <p>Copyright: Copyright (c) 2013</p>
 * <p>Company: Covisint LLC</p>
 * @author Jonny Xu
 * @date Jul 20, 2017
 * @version 1.0
 */

public class FirstKafkaTopology {

	/**
	 *  Running Topology on local cluster:
	 *  	 mvn compile exec:java -Dexec.classpathScope=compile -Dexec.mainClass=com.jonny.storm.FirstKafkaTopology
	 *  
	 *  Deploy Topology to Storm cluster:
	 *  	./storm/bin/storm jar first-kafka-topology-1.0-SNAPSHOT-jar-with-dependencies.jar com.jonny.storm.FirstKafkaTopology FirstKafkaTopology
	 *  
	 *  Stopping Storm Topology:
	 *  	./storm/bin/storm kill FirstKafkaTopology 
	 *  
	 *  Publish messages:
	 *  	./kafka/bin/kafka-console-producer.sh --broker-list 10.10.60.249:9092,10.10.60.235:9092,10.10.60.236:9092 --topic Messaging
	 *  
	 *  Consume messages:
	 *  	./kafka/bin/kafka-console-consumer.sh --zookeeper 10.10.60.249:2181,10.10.60.235:2181,10.10.60.236:2181 --topic Messaging --from-beginning
	 *  
	 * @param args
	 * @throws AlreadyAliveException
	 * @throws InvalidTopologyException
	 * @throws AuthorizationException
	 */

	public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException, AuthorizationException {
		String zkHosts = "10.10.60.249:2181,10.10.60.235:2181,10.10.60.236:2181";
		String messagingTopic = "Messaging";
		String zkRoot = "/storm"; // default zookeeper root configuration for storm
		String id = "KafkaMessagingSpout"; // 进度记录的id，想要一个新的Spout读取之前的记录，应把它的id设为跟之前的一样。
		
		/**
		 * 定义拓扑
		 */
		TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout("KafkaMessagingSpout", new KafkaSpout(getSpoutConf(new ZkHosts(zkHosts), messagingTopic, zkRoot, id)), 3);

		/**
		 *  在spout和bolts之间通过shuffleGrouping方法连接。这种分组方式决定了Storm会以随机分配方式从源节点向目标节点发送消息。
		 *  
		 *  Storm中8种流分组的方式：
		 *  fieldsGrouping（字段分组）：根据指定字段对流进行分组
		 *  globalGrouping（全局分组）：全部流发送到同一个Bolt中
		 *  shuffleGrouping（随机分组）：最常用的分组方式，随机分发元组
		 *  localOrShuffleGrouping（本地或者随机分组）：如果目标Bolt在同一工作进程存在一个或多个任务，会随机分配元组给这些任务
		 *  noneGrouping（无分组）：同随机分组
		 *  allGrouping（广播分组）：将分发流到所有的Bolt中，常用于更新缓存
		 *  directGrouping（直接分组）：只能在已声明为直接流的流中所使用，并且元组必须使用emitDirect方法来发射
		 *  customGrouping（自定义分组）：实现CustomStreamGrouping接口来创建自定义的流分组
		 */
		builder.setBolt("KafkaMessagingBolt", new KafkaMessagingBolt(), 2).localOrShuffleGrouping("KafkaMessagingSpout");

		/**
		 * 配置拓扑
		 * 由于是在开发阶段，设置debug属性为true，Storm会打印节点间交换的所有消息，以及其它有助于理解拓扑运行方式的调试数据
		 */
		Config conf = new Config();
		conf.setDebug(true);

		/**
		 * 运行拓扑
		 */
		if (args != null && args.length > 0) {
			conf.setNumWorkers(3);
			StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
		} else {
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("kafkaTest", conf, builder.createTopology());
			Utils.sleep(10000);
			cluster.killTopology("kafkaTest");
			cluster.shutdown();
		}
	}

	private static SpoutConfig getSpoutConf(final ZkHosts zkHosts, final String topic, final String zkRoot, final String grouping) {
		return getSpoutConf(zkHosts, topic, zkRoot, grouping, new SchemeAsMultiScheme(new StringScheme()));
	}

	private static SpoutConfig getSpoutConf(final ZkHosts zkHosts, final String topic, final String zkRoot, final String grouping, final MultiScheme scheme) {
		final SpoutConfig spoutConfig = new SpoutConfig(zkHosts, topic, zkRoot, grouping);
		spoutConfig.scheme = scheme;

		/**
		 * 指定从何时的offset时间开始读取数据，默认为最旧的offset
		 * -2: EarliestTime，是从kafka的头开始 
		 * -1: LatestTime，是从最新的开始 
		 *  0: =无，从ZK开始
		 */
		spoutConfig.startOffsetTime = OffsetRequest.LatestTime();

		return spoutConfig;
	}

}
