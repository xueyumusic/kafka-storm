package stormtry;

import java.util.List;
import java.util.ArrayList;
import backtype.storm.Config;
import backtype.storm.topology.TopologyBuilder;
import storm.kafka.SpoutConfig;
import storm.kafka.KafkaSpout;
import storm.kafka.ZkHosts;
import backtype.storm.StormSubmitter;
import org.apache.storm.guava.collect.ImmutableList;
import backtype.storm.LocalCluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Kstest {
  private static final Logger LOG = LoggerFactory.getLogger(Kstest.class);

  private final TopologyBuilder builder;
  private final Config topologyConfig;
  private final String topologyName;
  
  public Kstest(String topologyName) throws InterruptedException {
    builder = new TopologyBuilder();
    this.topologyName = topologyName;
    topologyConfig = createTopologyConfiguration();
    WireTopology();
  }

  private static Config createTopologyConfiguration() { 
    Config conf = new Config();
    conf.setDebug(true);
    return conf;
  }

  private void WireTopology() throws InterruptedException {
    String spoutId = "kafkaGenerator";
    String boltId = "kafkaBolt";
    List<String> hosts = new ArrayList<String>();
    hosts.add("172.18.14.101");
    //hosts.add("172.18.14.101:9093");
    //hosts.add("172.18.14.101:9094");

    List<String> zkServers = new ArrayList<String>();
    zkServers.add("172.18.14.101");
    /*SpoutConfig spoutConf = new SpoutConfig(KafkaConfig.StaticHosts.fromHostString(hosts, 1), "xueyutest", "/storm", "ksid");
    spoutConf.zkServers = zkServers;
    spoutConf.zkPort = 2181;
    spoutConf.forceStartOffsetTime(-2);*/
    
    SpoutConfig spoutConf = new SpoutConfig(new ZkHosts("172.18.14.101:2181", "/brokers"), "kstesttopic", "/mykstest", "ksid");
    /*SpoutConfig spoutConfig = new SpoutConfig(
      ImmutableList.of("172.18.14.101"), // list of Kafka brokers
      1, // number of partitions per host
      "xueyutest", // topic to read from
      "/xueyukafkastorm", // the root path in Zookeeper for the spout to store the consumer offsets
      "kdid");*/ // an id for this consumer for storing the consumer offsets in Zookeeper 
    KafkaSpout kafkaSpout = new KafkaSpout(spoutConf);
    builder.setSpout(spoutId, kafkaSpout);
    builder.setBolt(boltId, new StubBolt()).globalGrouping(spoutId);
  }

  public void runRemotely() throws Exception {
    //StormRunner.runTopologyRemotely(builder.createTopology(), topologyName, topologyConfig);
    StormSubmitter.submitTopology(topologyName, topologyConfig, builder.createTopology());
  }

  public void runLocal() throws Exception {
    LOG.info("######run Local");
    LocalCluster cluster = new LocalCluster();
    cluster.submitTopology(topologyName, topologyConfig, builder.createTopology());
    Thread.sleep(100000000);
    cluster.shutdown();
  }

  public static void main(String[] args) throws Exception {
    String topologyName = "KstestTopo";
    if (args.length >= 2) {
      topologyName =  args[1];
    }

    Kstest kst = new Kstest(topologyName);
    if (args[0].equals("local")) {
      kst.runLocal();
      
    } else {
        
      kst.runRemotely();
    }
  }
}
