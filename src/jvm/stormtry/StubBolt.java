package stormtry;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import backtype.storm.LocalCluster;
import java.io.File;
import java.io.FileOutputStream;
import java.io.*;
import java.util.Map;
import java.lang.Exception;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class StubBolt extends BaseBasicBolt {


  //FileWriter fw;
  

    /*@Override
    public Map<String, Object> getComponentConfiguration() {
            try{
    fw = new FileWriter("/tmp/storm-test/s1.log");
    } catch (Exception io) {
    }
        return null;
    } */
private static final Logger LOG = LoggerFactory.getLogger(StubBolt.class);

  @Override
  public void execute(Tuple tuple, BasicOutputCollector collector) {
    System.out.println(tuple);
    FileWriter fw = null;
    try {
      LOG.info("####i am here");
      Object obj = tuple.getValue(0);
      LOG.info("##tuple"+tuple.toString());
      LOG.info("##tuple obj:"+obj.toString());
      fw = new FileWriter("/tmp/storm-test/s1.log", true);
      fw.write(obj.toString());
      fw.write("\n");
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      try {
       fw.close();
      } catch (Exception e){
        e.printStackTrace();
      }
    }
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer ofd) {
  }

}

