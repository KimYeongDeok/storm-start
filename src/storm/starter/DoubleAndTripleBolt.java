package storm.starter;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import storm.starter.spout.RandomSentenceSpout;

import java.util.Map;

/**
 * Description.
 *
 * @author Youngdeok Kim
 * @since 1.0
 */
public class DoubleAndTripleBolt extends BaseRichBolt {
    private OutputCollector _collector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
        _collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        int val = input.getInteger(0);
        _collector.emit(input, new Values(val*2, val*3));
        _collector.ack(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("double", "triple"));
    }

    public static void main(String[] args) throws Exception {

           TopologyBuilder builder = new TopologyBuilder();

           builder.setSpout("spout", new RandomSentenceSpout(), 5);

           builder.setBolt("split", new DoubleAndTripleBolt(), 8);

           Config conf = new Config();
           conf.setDebug(true);


           if(args!=null && args.length > 0) {
               conf.setNumWorkers(3);

               StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
           } else {
               conf.setMaxTaskParallelism(3);

               LocalCluster cluster = new LocalCluster();
               cluster.submitTopology("word-count", conf, builder.createTopology());

               Thread.sleep(10000);

               cluster.shutdown();
           }
       }
}