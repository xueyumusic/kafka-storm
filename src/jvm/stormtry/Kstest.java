package stormtry;

import java.util.List;
import java.util.ArrayList;
import backtype.storm.Config;
import backtype.storm.topology.TopologyBuilder;
import storm.kafka.SpoutConfig;
import storm.kafka.KafkaSpout;
import storm.kafka.KafkaConfig;
import backtype.storm.StormSubmitter;
import org.apache.storm.guava.collect.ImmutableList;


public class Kstest {
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
    SpoutConfig spoutConf = new SpoutConfig(KafkaConfig.StaticHosts.fromHostString(hosts, 1), "xueyutest", "/xueyukafkastorm", "ksid");
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

  public static void main(String[] args) throws Exception {
    String topologyName = "KstestTopo";
    if (args.length >= 1) {
      topologyName =  args[0];
    }
    
    Kstest kst = new Kstest(topologyName);
    kst.runRemotely();
  }
}
